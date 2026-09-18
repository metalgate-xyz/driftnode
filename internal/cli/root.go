// Package cli implements the offline driftnode CLI commands (Phase 0,
// step 2): init, whoami, post, feed, follow/unfollow, key export/import, and
// backup export/import. All commands operate directly on the local bbolt
// store with no daemon or network.
package cli

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	bootstrappkg "driftnode/internal/bootstrap"
	"driftnode/internal/core"
	"driftnode/internal/daemon"
	"driftnode/internal/store"
	"driftnode/internal/tui"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// openStore opens the store at the given path, creating the parent directory.
func openStoreAt(path string) (*store.Store, error) {
	if path == "" {
		return nil, fmt.Errorf("no store path: pass --db <path>")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	s, err := store.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	return s, nil
}

// dbPath holds the --db flag value. It is a required local flag on every
// command that opens the store or talks to the daemon; store-less commands
// (bootstrap keygen/sign/verify, relay) don't define it.
var dbPath string

// addDBFlag adds a required --db local flag to a standalone command.
func addDBFlag(c *cobra.Command) {
	c.Flags().StringVar(&dbPath, "db", "", "path to the local store")
	_ = c.MarkFlagRequired("db")
}

// addDBFlagPersistent adds a required --db persistent flag to a parent
// command so its subcommands inherit it.
func addDBFlagPersistent(c *cobra.Command) {
	c.PersistentFlags().StringVar(&dbPath, "db", "", "path to the local store")
	_ = c.MarkPersistentFlagRequired("db")
}

// Root is the root command.
func Root() *cobra.Command {
	root := &cobra.Command{
		Use:   "driftnode",
		Short: "driftnode native peer node (offline, Phase 0)",
	}
	// Cobra writes command output (cmd.Println) to stderr by default.
	// All our commands produce user-facing output that belongs on stdout
	// for shell pipelines and command substitution.
	root.SetOut(os.Stdout)
	root.AddCommand(
		initCmd(),
		whoamiCmd(),
		postCmd(),
		feedCmd(),
		followCmd(),
		unfollowCmd(),
		keyCmd(),
		backupCmd(),
		daemonCmd(),
		tuiCmd(),
		peersCmd(),
		syncCmd(),
		bootstrapCmd(),
		relayCmd(),
	)
	return root
}

func initCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "init",
		Short: "Generate an Ed25519 identity and create the local store",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if passphrase == "" {
				return fmt.Errorf("--passphrase is required")
			}
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()

			kp, err := core.NewKeyPair()
			if err != nil {
				return fmt.Errorf("generate keypair: %w", err)
			}
			enc := core.DefaultKeyEncryption()
			ek, err := enc.Encrypt(kp.Private, []byte(passphrase))
			if err != nil {
				return fmt.Errorf("encrypt key: %w", err)
			}
			if err := s.InitIdentity(kp, ek); err != nil {
				return fmt.Errorf("init identity: %w", err)
			}
			cmd.Println(kp.Identity())
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to encrypt the private key at rest")
	return c
}

func whoamiCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "whoami",
		Short: "Print the identity's public key and fingerprint",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err == nil {
				if resp, err := daemon.SendRequest(sock, "whoami", nil); err == nil {
					if m, ok := resp.Result.(map[string]any); ok {
						if id, ok := m["identity"].(string); ok {
							cmd.Println(id)
							return nil
						}
					}
				}
			}
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			id, ok, err := s.Identity()
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("no identity found; run 'driftnode init' first")
			}
			cmd.Println(id)
			return nil
		},
	}
	addDBFlag(c)
	return c
}

func postCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "post <text>",
		Short: "Append a signed Post event to the local PostLog",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if passphrase == "" {
				return fmt.Errorf("--passphrase is required")
			}
			// When the daemon is running, post via the control socket.
			sock, err := daemonSocketPath()
			if err == nil {
				if resp, err := daemon.SendRequest(sock, "post", map[string]any{"text": args[0], "passphrase": passphrase}); err == nil {
					if m, ok := resp.Result.(map[string]any); ok {
						if id, ok := m["event_id"].(string); ok {
							cmd.Println(id)
							return nil
						}
					}
					return fmt.Errorf("unexpected post response: %v", resp.Result)
				}
			}
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			kp, err := loadKey(s, passphrase)
			if err != nil {
				return err
			}
			seq, err := s.OwnEventCount(core.PostLog)
			if err != nil {
				return fmt.Errorf("get sequence: %w", err)
			}
			se, err := kp.Sign(core.Event{
				Kind:      core.KindPost,
				Log:       core.PostLog,
				Timestamp: core.Now64(),
				Sequence:  seq + 1,
				Post:      &core.Post{Text: args[0]},
			})
			if err != nil {
				return fmt.Errorf("sign: %w", err)
			}
			if err := s.AppendOwnEvent(core.PostLog, se); err != nil {
				return fmt.Errorf("append: %w", err)
			}
			id, _ := se.ID()
			cmd.Println(id)
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to unlock the private key")
	return c
}

func feedCmd() *cobra.Command {
	var limit int
	c := &cobra.Command{
		Use:   "feed",
		Short: "Print the merged timeline from local state (no network)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// When the daemon is running (it holds the store lock),
			// query the merged feed via the control socket. Otherwise
			// open the store directly for offline use.
			sock, err := daemonSocketPath()
			if err == nil {
				if resp, err := daemon.SendRequest(sock, "feed", map[string]any{"limit": limit}); err == nil {
					return printFeedFromRPC(cmd, resp)
				}
			}
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			allPosts, err := s.AllPosts()
			if err != nil {
				return fmt.Errorf("read posts: %w", err)
			}
			log := core.NewLog(allPosts)
			posts := log.Posts()
			if limit > 0 && len(posts) > limit {
				posts = posts[:limit]
			}
			for _, p := range posts {
				ts := core.FormatTime(p.Event.Timestamp)
				author := p.Author.String()
				short := author
				if len(short) > 16 {
					short = short[:16]
				}
				text := p.Event.Post.Text
				if p.Event.Reply != nil {
					text = "(reply) " + text
				}
				cmd.Printf("%s  %s> %s\n", ts, short, text)
			}
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().IntVarP(&limit, "limit", "n", 0, "maximum number of posts to show (0 = all)")
	return c
}

// printFeedFromRPC renders feed items from a daemon RPC response.
func printFeedFromRPC(cmd *cobra.Command, resp *daemon.Response) error {
	items, ok := resp.Result.([]any)
	if !ok {
		return fmt.Errorf("unexpected feed response")
	}
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		ts := int64(m["timestamp"].(float64))
		author := m["author"].(string)
		short := author
		if len(short) > 16 {
			short = short[:16]
		}
		text := m["text"].(string)
		cmd.Printf("%s  %s> %s\n", core.FormatTime(ts), short, text)
	}
	return nil
}

func followCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "follow <pubkey>",
		Short: "Follow an identity (append a signed Follow event)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if passphrase == "" {
				return fmt.Errorf("--passphrase is required")
			}
			sock, err := daemonSocketPath()
			if err == nil {
				if resp, err := daemon.SendRequest(sock, "follow", map[string]any{"target": args[0], "passphrase": passphrase}); err == nil {
					if m, ok := resp.Result.(map[string]any); ok {
						if id, ok := m["followed"].(string); ok {
							cmd.Printf("followed %s\n", id)
							return nil
						}
					}
					return fmt.Errorf("unexpected follow response: %v", resp.Result)
				}
			}
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			kp, err := loadKey(s, passphrase)
			if err != nil {
				return err
			}
			target, err := resolvePubkey(args[0])
			if err != nil {
				return err
			}
			seq, err := s.OwnEventCount(core.ProfileLog)
			if err != nil {
				return fmt.Errorf("get sequence: %w", err)
			}
			se, err := kp.Sign(core.Event{
				Kind:      core.KindFollow,
				Log:       core.ProfileLog,
				Timestamp: core.Now64(),
				Sequence:  seq + 1,
				Follow:    &core.Follow{TargetPubkey: target},
			})
			if err != nil {
				return fmt.Errorf("sign: %w", err)
			}
			if err := s.AppendOwnEvent(core.ProfileLog, se); err != nil {
				return fmt.Errorf("append: %w", err)
			}
			cmd.Printf("followed %s\n", core.IdentityFromPubkey(ed25519.PublicKey(target[:])))
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to unlock the private key")
	return c
}

func unfollowCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "unfollow <pubkey>",
		Short: "Unfollow an identity (append a signed Unfollow event)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if passphrase == "" {
				return fmt.Errorf("--passphrase is required")
			}
			sock, err := daemonSocketPath()
			if err == nil {
				if resp, err := daemon.SendRequest(sock, "unfollow", map[string]any{"target": args[0], "passphrase": passphrase}); err == nil {
					if m, ok := resp.Result.(map[string]any); ok {
						if id, ok := m["unfollowed"].(string); ok {
							cmd.Printf("unfollowed %s\n", id)
							return nil
						}
					}
					return fmt.Errorf("unexpected unfollow response: %v", resp.Result)
				}
			}
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			kp, err := loadKey(s, passphrase)
			if err != nil {
				return err
			}
			target, err := resolvePubkey(args[0])
			if err != nil {
				return err
			}
			seq, err := s.OwnEventCount(core.ProfileLog)
			if err != nil {
				return fmt.Errorf("get sequence: %w", err)
			}
			se, err := kp.Sign(core.Event{
				Kind:      core.KindUnfollow,
				Log:       core.ProfileLog,
				Timestamp: core.Now64(),
				Sequence:  seq + 1,
				Follow:    &core.Follow{TargetPubkey: target},
			})
			if err != nil {
				return fmt.Errorf("sign: %w", err)
			}
			if err := s.AppendOwnEvent(core.ProfileLog, se); err != nil {
				return fmt.Errorf("append: %w", err)
			}
			cmd.Printf("unfollowed %s\n", core.IdentityFromPubkey(ed25519.PublicKey(target[:])))
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to unlock the private key")
	return c
}

func keyCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "key",
		Short: "Export or import the encrypted keypair",
	}
	addDBFlagPersistent(c)
	c.AddCommand(keyExportCmd(), keyImportCmd())
	return c
}

func keyExportCmd() *cobra.Command {
	var outPath string
	c := &cobra.Command{
		Use:   "export",
		Short: "Export the encrypted keypair to a file",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			ek, err := s.EncryptedKey()
			if err != nil {
				return fmt.Errorf("read key: %w", err)
			}
			b, err := core.CanonicalEncode(ek)
			if err != nil {
				return fmt.Errorf("encode: %w", err)
			}
			if outPath == "" {
				outPath = "driftnode-key.cbor"
			}
			if err := os.WriteFile(outPath, b, 0o600); err != nil {
				return fmt.Errorf("write: %w", err)
			}
			cmd.Printf("exported encrypted key to %s\n", outPath)
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&outPath, "out", "o", "", "output path (default: driftnode-key.cbor)")
	return c
}

func keyImportCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "import <path>",
		Short: "Import an encrypted keypair from a file, replacing the current one",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if passphrase == "" {
				return fmt.Errorf("--passphrase is required (needed to derive and store the identity)")
			}
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			b, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("read: %w", err)
			}
			var ek core.EncryptedKey
			if err := core.CanonicalDecode(b, &ek); err != nil {
				return fmt.Errorf("decode: %w", err)
			}
			if err := s.PutEncryptedKey(&ek); err != nil {
				return fmt.Errorf("store key: %w", err)
			}
			// Decrypt to derive the public key and store the identity.
			enc := core.DefaultKeyEncryption()
			priv, err := enc.Decrypt(&ek, []byte(passphrase))
			if err != nil {
				return fmt.Errorf("decrypt key: %w", err)
			}
			kp, err := core.KeyPairFromBytes(priv)
			if err != nil {
				return fmt.Errorf("reconstruct keypair: %w", err)
			}
			if err := s.SetIdentity(kp.Identity()); err != nil {
				return fmt.Errorf("store identity: %w", err)
			}
			cmd.Printf("imported key for %s\n", kp.Identity())
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to decrypt the key and derive the identity")
	return c
}

func backupCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "backup",
		Short: "Export or import a single-file account backup",
	}
	addDBFlagPersistent(c)
	c.AddCommand(backupExportCmd(), backupImportCmd())
	return c
}

func backupExportCmd() *cobra.Command {
	var outPath string
	c := &cobra.Command{
		Use:   "export",
		Short: "Write the single-file CBOR backup (Section 8)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			bd, err := s.ExportBackup()
			if err != nil {
				return fmt.Errorf("export: %w", err)
			}
			b, err := core.CanonicalEncode(bd)
			if err != nil {
				return fmt.Errorf("encode: %w", err)
			}
			if outPath == "" {
				outPath = "driftnode-backup.cbor"
			}
			if err := os.WriteFile(outPath, b, 0o600); err != nil {
				return fmt.Errorf("write: %w", err)
			}
			cmd.Printf("exported backup to %s\n", outPath)
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&outPath, "out", "o", "", "output path (default: driftnode-backup.cbor)")
	return c
}

func backupImportCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "import <path>",
		Short: "Restore or merge from a backup file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			b, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("read: %w", err)
			}
			var bd core.BackupData
			if err := core.CanonicalDecode(b, &bd); err != nil {
				return fmt.Errorf("decode: %w", err)
			}
			if err := s.ImportBackup(&bd); err != nil {
				return fmt.Errorf("import: %w", err)
			}
			cmd.Println("imported backup")
			return nil
		},
	}
	return c
}

// daemonSocketPath returns the control socket path for the given --db path.
// Each store gets its own socket so multiple daemons don't collide.
func daemonSocketPath() (string, error) {
	if dbPath == "" {
		return "", fmt.Errorf("no store path: pass --db <path>")
	}
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return "", fmt.Errorf("resolve db path: %w", err)
	}
	return daemon.SocketPathFor(abs)
}

// passphrase, returning a ready-to-sign KeyPair.
func loadKey(s *store.Store, passphrase string) (*core.KeyPair, error) {
	ek, err := s.EncryptedKey()
	if err != nil {
		return nil, fmt.Errorf("read encrypted key: %w", err)
	}
	enc := core.DefaultKeyEncryption()
	priv, err := enc.Decrypt(ek, []byte(passphrase))
	if err != nil {
		return nil, fmt.Errorf("decrypt key: %w", err)
	}
	return core.KeyPairFromBytes(priv)
}

// --- daemon and network commands ---

func daemonCmd() *cobra.Command {
	var foreground bool
	var keyFile string
	var bootstrapFile string
	var bootstrapKeyFile string
	c := &cobra.Command{
		Use:   "daemon",
		Short: "Start the long-running daemon and control socket",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
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
			if keyFile != "" {
				d.SetKeyFile(keyFile)
			}
			if bootstrapFile != "" {
				verifyKey, err := decodeBootstrapKey(bootstrapKeyFile)
				if err != nil {
					return err
				}
				d.SetBootstrap(bootstrapFile, verifyKey)
			}
			if err := d.Start(sock); err != nil {
				return err
			}
			cmd.Println("daemon started on", sock)
			if !foreground {
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
	c.AddCommand(daemonStopCmd(), daemonStatusCmd())
	c.Flags().BoolVarP(&foreground, "foreground", "f", false, "run in foreground with logs on stdout")
	c.Flags().StringVar(&keyFile, "key", "", "path to a persistent tailcat key file (stable address token across restarts)")
	c.Flags().StringVar(&bootstrapFile, "bootstrap", "", "path to a signed bootstrap.yaml to load and auto-dial seed peers")
	c.Flags().StringVar(&bootstrapKeyFile, "bootstrap-key", "", "path to a file containing the base64 Ed25519 public key that signed the bootstrap")
	return c
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
			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			_, err = daemon.SendRequest(sock, "stop", nil)
			if err != nil {
				return err
			}
			cmd.Println("daemon stopping")
			return nil
		},
	}
}

func daemonStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show whether the daemon is running",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			resp, err := daemon.SendRequest(sock, "status", nil)
			if err != nil {
				cmd.Println("not running")
				return nil
			}
			result, ok := resp.Result.(map[string]any)
			if !ok {
				return fmt.Errorf("unexpected status response")
			}
			cmd.Printf("running: %v\n", result["running"])
			cmd.Printf("socket: %v\n", result["socket"])
			cmd.Printf("peers: %v\n", result["peers"])
			cmd.Printf("transport: %v\n", result["transport"])
			if addr, _ := result["listen_addr"].(string); addr != "" {
				cmd.Printf("listen_addr: %v\n", addr)
			}
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
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			id, ok, err := s.Identity()
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("no identity found; run 'driftnode init' first")
			}
			return tui.Run(id.String())
		},
	}
	addDBFlag(c)
	return c
}

func peersCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "peers",
		Short: "Manage peer connections",
	}
	addDBFlagPersistent(c)
	c.AddCommand(peersListCmd(), peersAddCmd(), peersTokenCmd())
	return c
}

func peersTokenCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "token",
		Short: "Print this node's tailcat address token for peers to dial",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			resp, err := daemon.SendRequest(sock, "token", nil)
			if err != nil {
				return err
			}
			result, ok := resp.Result.(map[string]any)
			if !ok {
				return fmt.Errorf("unexpected token response")
			}
			token, _ := result["token"].(string)
			if token == "" {
				cmd.Println("(transport unavailable; run 'driftnode daemon')")
				return nil
			}
			cmd.Println(token)
			return nil
		},
	}
}

func peersListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "Show currently connected peers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			resp, err := daemon.SendRequest(sock, "peers", nil)
			if err != nil {
				return err
			}
			peers, ok := resp.Result.([]any)
			if !ok || len(peers) == 0 {
				cmd.Println("(no peers connected)")
				return nil
			}
			for _, p := range peers {
				pm, ok := p.(map[string]any)
				if !ok {
					continue
				}
				cmd.Printf("%s %s (%s)\n", pm["status"], pm["id"], pm["kind"])
			}
			return nil
		},
	}
}

func peersAddCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "add <token>",
		Short: "Manually connect to a peer's Tailcat token",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			_, err = daemon.SendRequest(sock, "peers_add", map[string]any{"token": args[0]})
			if err != nil {
				return err
			}
			cmd.Println("connecting to", args[0])
			return nil
		},
	}
}

func syncCmd() *cobra.Command {
	var now bool
	c := &cobra.Command{
		Use:   "sync",
		Short: "Trigger a sync round",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			_, err = daemon.SendRequest(sock, "sync", map[string]any{"now": now})
			if err != nil {
				return err
			}
			cmd.Println("sync triggered")
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().BoolVar(&now, "now", false, "trigger immediate sync instead of waiting for the interval")
	return c
}

func bootstrapCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "bootstrap",
		Short: "Manage the bootstrap file",
	}
	c.AddCommand(bootstrapKeygenCmd(), bootstrapSignCmd(), bootstrapVerifyCmd())
	return c
}

// bootstrapKeygenCmd generates an Ed25519 keypair for signing bootstrap
// files. The private key (64-byte raw, seed||pubkey) goes to --key-out; the
// base64 public key is printed to stdout for use with `verify --key` and
// `daemon --bootstrap-key`. With --key it instead derives the public key
// from an existing private key without generating or writing anything.
func bootstrapKeygenCmd() *cobra.Command {
	var keyOut, keyIn string
	c := &cobra.Command{
		Use:   "keygen",
		Short: "Generate an Ed25519 keypair, or derive the public key from an existing private key",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if keyIn != "" {
				keyData, err := os.ReadFile(keyIn)
				if err != nil {
					return fmt.Errorf("read key: %w", err)
				}
				priv := ed25519.PrivateKey(keyData)
				if len(priv) != ed25519.PrivateKeySize {
					return fmt.Errorf("key file: want %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
				}
				pub, ok := priv.Public().(ed25519.PublicKey)
				if !ok {
					return fmt.Errorf("invalid ed25519 private key")
				}
				cmd.Println(base64.StdEncoding.EncodeToString(pub))
				return nil
			}
			if keyOut == "" {
				return fmt.Errorf("--key-out is required (or use --key to derive a public key)")
			}
			pub, priv, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				return fmt.Errorf("generate key: %w", err)
			}
			if err := os.WriteFile(keyOut, priv, 0o600); err != nil {
				return fmt.Errorf("write private key: %w", err)
			}
			cmd.Println(base64.StdEncoding.EncodeToString(pub))
			return nil
		},
	}
	c.Flags().StringVar(&keyOut, "key-out", "", "path to write the 64-byte raw Ed25519 private key")
	c.Flags().StringVar(&keyIn, "key", "", "path to an existing private key (derive its public key instead of generating)")
	return c
}

func bootstrapSignCmd() *cobra.Command {
	var keyFile string
	c := &cobra.Command{
		Use:   "sign <file>",
		Short: "Sign a bootstrap.yaml with an Ed25519 private key file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bf, err := bootstrappkg.Load(args[0])
			if err != nil {
				return err
			}
			keyData, err := os.ReadFile(keyFile)
			if err != nil {
				return fmt.Errorf("read key: %w", err)
			}
			priv := ed25519.PrivateKey(keyData)
			if len(priv) != ed25519.PrivateKeySize {
				return fmt.Errorf("key file: want %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
			}
			if err := bf.Sign(priv); err != nil {
				return err
			}
			out, err := yaml.Marshal(bf)
			if err != nil {
				return err
			}
			if err := os.WriteFile(args[0], out, 0o600); err != nil {
				return err
			}
			cmd.Printf("signed %s\n", args[0])
			return nil
		},
	}
	c.Flags().StringVar(&keyFile, "key", "", "path to a 64-byte raw Ed25519 private key file")
	_ = c.MarkFlagRequired("key")
	return c
}

func bootstrapVerifyCmd() *cobra.Command {
	var keyFile string
	c := &cobra.Command{
		Use:   "verify <file>",
		Short: "Verify a bootstrap.yaml's signature",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bf, err := bootstrappkg.Load(args[0])
			if err != nil {
				return err
			}
			cmd.Printf("version: %d\n", bf.Version)
			cmd.Printf("seed_relays: %d\n", len(bf.SeedRelays))
			cmd.Printf("derp_relays: %d\n", len(bf.DERPRelays))
			cmd.Printf("seed_peers: %d\n", len(bf.SeedPeers))
			cmd.Printf("crawl_seeds: %d\n", len(bf.CrawlSeeds))
			if bf.Signature == "" {
				cmd.Println("signature: (none)")
				return nil
			}
			if keyFile == "" {
				cmd.Println("signature: present (no --key provided to verify against)")
				return nil
			}
			pub, err := decodeBootstrapKey(keyFile)
			if err != nil {
				return err
			}
			if err := bf.Verify(pub); err != nil {
				return fmt.Errorf("signature verification failed: %w", err)
			}
			cmd.Println("signature: verified")
			return nil
		},
	}
	c.Flags().StringVar(&keyFile, "key", "", "path to a file containing the base64 Ed25519 public key to verify against")
	return c
}

func relayCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "relay",
		Short: "Manage the relay/bootstrap role",
	}
	c.AddCommand(relayStatusCmd(), relayEnableCmd(), relayDisableCmd())
	return c
}

func relayStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show reachability check result and relay role status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Println("relay role: ephemeral (default)")
			cmd.Println("(use 'relay enable' to advertise as a bootstrap candidate)")
			return nil
		},
	}
}

func relayEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "enable",
		Short: "Opt in to advertising this node as a relay/bootstrap candidate",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Println("relay enable: not yet implemented (requires DHT integration)")
			return nil
		},
	}
}

func relayDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable",
		Short: "Opt out of advertising this node as a relay/bootstrap candidate",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Println("relay disable: not yet implemented")
			return nil
		},
	}
}

// resolvePubkey accepts either a full driftnode:<pubkey> identity string or a
// bare base32 pubkey and returns the raw 32 bytes.
func resolvePubkey(arg string) ([32]byte, error) {
	var s string
	if id, err := core.ParseIdentity(arg); err == nil {
		pub, err := id.PubkeyBytes()
		if err != nil {
			return [32]byte{}, err
		}
		return [32]byte(pub), nil
	}
	s = arg
	pub, err := core.PubkeyFromBase32(s)
	if err != nil {
		return [32]byte{}, fmt.Errorf("invalid pubkey %q: %w", arg, err)
	}
	return [32]byte(pub), nil
}
