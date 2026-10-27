package cli

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"driftnode/internal/daemon"
	driftnodepb "driftnode/internal/proto/driftnodepb"
	"driftnode/internal/tui"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// --- daemon and network commands ---

func daemonCmd() *cobra.Command {

	var detached bool
	var detachedChild bool
	var logFile string
	var ephemeral bool
	var bootstrapFile string
	var bootstrapKeyFile string
	var idleLockStr string
	var c *cobra.Command
	c = &cobra.Command{
		Use:   "daemon",
		Short: "Start the long-running daemon and control socket",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// --detached forks a background child (in a new session) and
			// returns immediately; the parent exits 0. The child re-runs
			// this command with the internal --detached-child marker so it
			// does not fork again, and runs in foreground (blocking) mode.
			// The child's stdio is already pointed at the log file by the
			// parent, so slog output is captured without further setup.
			if detached && !detachedChild {
				return spawnDetachedDaemon(cmd, c, logFile)
			}

			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()

			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			d := daemon.New(s, nil)
			if ephemeral {
				d.SetEphemeralKey()
			}
			if bootstrapFile != "" {
				verifyKey, err := decodeBootstrapKey(bootstrapKeyFile)
				if err != nil {
					return err
				}
				d.SetBootstrap(bootstrapFile, verifyKey)
			}
			if idleLockStr != "" {
				dur, err := time.ParseDuration(idleLockStr)
				if err != nil {
					return fmt.Errorf("--idle-lock: %w", err)
				}
				d.SetIdleLock(dur)
			}
			// When started interactively with a TTY, decrypt the signing
			// key before Start so the daemon can sync, crawl, and run
			// bootstrap auto-follow from the first moment. Non-interactive
			// starts (no TTY, e.g. detached in a container) stay locked;
			// the 'daemon unlock' RPC covers those.
			if _, err := s.EncryptedKey(); err == nil && term.IsTerminal(int(os.Stdin.Fd())) {
				passphrase, err := readPassphrase("passphrase: ")
				if err != nil {
					return err
				}
				if passphrase != "" {
					kp, err := loadKey(s, passphrase)
					if err != nil {
						return err
					}
					d.SetUnlockedKey(kp)
					cmd.Println("key unlocked")
				}
			}
			if err := d.Start(sock); err != nil {
				return err
			}
			cmd.Println("daemon started on", sock)
			if detachedChild {
				cmd.Println("logs:", logFile)
			} else {
				cmd.Println("press Ctrl+C to stop")
			}
			sig := make(chan os.Signal, 1)
			signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
			select {
			case <-sig:
				d.Stop()
			case <-d.Done():
			}
			return nil
		},
	}
	addDBFlagPersistent(c)
	c.AddCommand(daemonStopCmd(), daemonRestartCmd(), daemonStatusCmd(), daemonUnlockCmd(), daemonLockCmd())
	c.Flags().BoolVarP(&detached, "detached", "d", false, "run in the background (default: foreground)")
	c.Flags().BoolVar(&detachedChild, "detached-child", false, "internal: marks the background child forked by --detached")
	_ = c.Flags().MarkHidden("detached-child")
	c.Flags().StringVar(&logFile, "log", "", "log file path for --detached (default: a file next to the control socket)")
	c.Flags().BoolVar(&ephemeral, "ephemeral", false, "generate a fresh address token each run (do not persist the transport key)")
	c.Flags().StringVar(&bootstrapFile, "bootstrap", "", "path to a signed bootstrap.yaml to load and auto-dial seed zens")
	c.Flags().StringVar(&bootstrapKeyFile, "bootstrap-key", "", "path to a file containing the base64 Ed25519 public key that signed the bootstrap")
	c.Flags().StringVar(&idleLockStr, "idle-lock", "", "auto-lock the signing key after this idle duration (e.g. 5m, 1h); default keeps it unlocked until 'daemon lock' or stop")
	return c
}

// spawnDetachedDaemon re-executes the current binary as a background child in
// a new session, with stdio redirected to a log file, then returns so the
// parent exits. The child is marked with --detached-child so it runs in the
// foreground (blocking) branch and does not fork again. parent holds the
// start flags to forward (ephemeral, bootstrap, ...); logPath is the log file
// to capture the child's stdout/stderr (empty resolves a default next to the
// control socket). The child inherits the log file as its stdio, so the
// daemon's slog output is captured without further setup.
func spawnDetachedDaemon(cmd *cobra.Command, parent *cobra.Command, logPath string) error {
	if logPath == "" {
		logPath = defaultDaemonLogPath()
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return fmt.Errorf("create log dir: %w", err)
	}

	childArgs := []string{"--db", dbPath, "daemon", "--detached", "--detached-child", "--log", logPath}
	for _, f := range []string{"ephemeral", "bootstrap", "bootstrap-key", "idle-lock"} {
		if parent.Flags().Changed(f) {
			childArgs = append(childArgs, "--"+f, parent.Flags().Lookup(f).Value.String())
		}
	}

	bin, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}
	c := exec.Command(bin, childArgs...)
	c.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	devnull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err == nil {
		c.Stdin = devnull
	}
	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	c.Stdout = logf
	c.Stderr = logf
	if err := c.Start(); err != nil {
		return fmt.Errorf("start detached daemon: %w", err)
	}
	cmd.Printf("daemon started detached on %s\n", mustSocketPath())
	cmd.Printf("logs: %s\n", logPath)
	cmd.Println("use 'driftnode daemon stop' to stop it")
	return nil
}

// mustSocketPath returns the control socket path, or "(unknown)" if the db
// path is unset. Used after a successful fork to report the socket to the user.
func mustSocketPath() string {
	sock, err := daemonSocketPath()
	if err != nil {
		return "(unknown)"
	}
	return sock
}

// defaultDaemonLogPath returns a log file path next to the control socket,

// so a detached daemon's logs stay per-store.
func defaultDaemonLogPath() string {
	sock, err := daemonSocketPath()
	if err != nil || sock == "" {
		return filepath.Join(os.TempDir(), "driftnode-daemon.log")
	}
	dir := filepath.Dir(sock)
	h := sha256Of(dbPath)
	return filepath.Join(dir, "daemon-"+h+".log")
}

func sha256Of(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:8])
}

// readPassphrase reads a single line from the terminal without echoing it.
// It falls back to reading from stdin when the process has no TTY (e.g.
// piped input), in which case the input is visible.
func readPassphrase(prompt string) (string, error) {
	fmt.Fprint(os.Stderr, prompt)
	if term.IsTerminal(int(os.Stdin.Fd())) {
		b, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", fmt.Errorf("read passphrase: %w", err)
		}
		return string(b), nil
	}
	var line string
	if _, err := fmt.Scanln(&line); err != nil {
		return "", fmt.Errorf("read passphrase: %w", err)
	}
	return line, nil
}

// decodeBootstrapKey reads a base64 Ed25519 public key from a file. An empty
// path returns nil (no verification, for unsigned test bootstraps).
func decodeBootstrapKey(keyFile string) (ed25519.PublicKey, error) {
	if keyFile == "" {
		return nil, nil
	}
	b, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("read bootstrap key: %w", err)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
	if err != nil {
		return nil, fmt.Errorf("decode bootstrap key: %w", err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("bootstrap key: want %d bytes, got %d", ed25519.PublicKeySize, len(raw))
	}
	return ed25519.PublicKey(raw), nil
}

func daemonStopCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stop a running daemon via its control socket",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, cc, err := dialDaemon()
			if err != nil {
				return err
			}
			defer cc.Close()
			if _, err := cl.Stop(context.Background(), &driftnodepb.Empty{}); err != nil {
				return err
			}
			cmd.Println("daemon stopping")
			return nil
		},
	}
}

// daemonRestartCmd stops a running daemon, waits for its control socket to
// disappear, then starts a fresh detached daemon with the same start flags.
// It is the recovery path for a daemon whose tailcat connection has wedged:
// a fresh process rebinds the transport key and re-dials the follow graph.
func daemonRestartCmd() *cobra.Command {
	var logFile string
	var ephemeral bool
	var bootstrapFile string
	var bootstrapKeyFile string
	var idleLockStr string
	var c *cobra.Command
	c = &cobra.Command{
		Use:   "restart",
		Short: "Stop the running daemon and start a fresh one",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			// Stop the running daemon. A not-running error is fine: restart
			// can launch a fresh daemon even when none is up.
			if cl, cc, derr := dialDaemon(); derr == nil {
				if _, err := cl.Stop(context.Background(), &driftnodepb.Empty{}); err == nil {
					cc.Close()
					cmd.Println("daemon stopping")
					if err := waitForSocketGone(sock, 5*time.Second); err != nil {
						return fmt.Errorf("old daemon did not shut down: %w", err)
					}
				} else {
					cc.Close()
				}
			}
			return spawnDetachedDaemon(cmd, c, logFile)
		},
	}
	addDBFlag(c)
	c.Flags().StringVar(&logFile, "log", "", "log file path for the relaunched daemon (default: next to the control socket)")
	c.Flags().BoolVar(&ephemeral, "ephemeral", false, "generate a fresh address token each run (do not persist the transport key)")
	c.Flags().StringVar(&bootstrapFile, "bootstrap", "", "path to a signed bootstrap.yaml to load and auto-dial seed zens")
	c.Flags().StringVar(&bootstrapKeyFile, "bootstrap-key", "", "path to a file containing the base64 Ed25519 public key that signed the bootstrap")
	c.Flags().StringVar(&idleLockStr, "idle-lock", "", "auto-lock the signing key after this idle duration (e.g. 5m, 1h)")
	return c
}

// waitForSocketGone polls until the control socket no longer accepts
// connections, or the timeout elapses.
func waitForSocketGone(sock string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("unix", sock)
		if err != nil {
			return nil
		}
		conn.Close()
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("timeout")
}

func daemonStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show whether the daemon is running",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, cc, err := dialDaemon()
			if err != nil {
				cmd.Println("not running")
				return nil
			}
			defer cc.Close()
			resp, err := cl.Status(context.Background(), &driftnodepb.Empty{})
			if err != nil {
				cmd.Println("not running")
				return nil
			}
			cmd.Printf("running: %v\n", resp.Running)
			cmd.Printf("socket: %v\n", resp.Socket)
			cmd.Printf("zens: %v\n", resp.Zens)
			cmd.Printf("transport: %v\n", resp.Transport)
			cmd.Printf("unlocked: %v\n", resp.Unlocked)
			return nil
		},
	}
}

func tuiCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "tui",
		Short: "Launch the terminal UI",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			cl, cc, err := daemon.DialClient(sock)
			if err != nil {
				return fmt.Errorf("daemon not running; start it with 'driftnode daemon' first: %w", err)
			}
			defer cc.Close()
			whoami, err := cl.Whoami(context.Background(), &driftnodepb.Empty{})
			if err != nil {
				return fmt.Errorf("daemon not running; start it with 'driftnode daemon' first: %w", err)
			}
			if whoami.Identity == "" {
				return fmt.Errorf("no identity found; run 'driftnode init' first")
			}
			// Unlock the daemon's signing key once, so compose can post
			// without re-prompting. If already unlocked, the daemon keeps
			// the existing key.
			status, err := cl.Status(context.Background(), &driftnodepb.Empty{})
			if err == nil && !status.Unlocked {
				passphrase, err := readPassphrase("passphrase: ")
				if err != nil {
					return err
				}
				if _, err := cl.Unlock(context.Background(), &driftnodepb.UnlockReq{Passphrase: passphrase}); err != nil {
					return err
				}
			}
			return tui.Run(sock, whoami.Identity)
		},
	}
	addDBFlag(c)
	return c
}
