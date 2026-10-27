// Package cli implements the driftnode commands. Commands that touch a
// store are online-first: they call the daemon over its gRPC control socket
// and fall back to opening the bbolt store directly when no daemon is
// running. Store-less commands (bootstrap keygen/sign/verify, relay) work
// standalone.
package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"driftnode/internal/core"
	"driftnode/internal/daemon"
	driftnodepb "driftnode/internal/proto/driftnodepb"
	"driftnode/internal/store"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
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
		Short: "driftnode native zen node (offline, Phase 0)",
	}
	// Cobra writes command output (cmd.Println) to stderr by default.
	// All our commands produce user-facing output that belongs on stdout
	// for shell pipelines and command substitution.
	root.SetOut(os.Stdout)
	root.AddCommand(
		initCmd(),
		whoamiCmd(),
		postCmd(),
		likeCmd(),
		feedCmd(),
		followCmd(),
		unfollowCmd(),
		followsCmd(),
		followersCmd(),
		profileCmd(),
		detailCmd(),
		keyCmd(),
		backupCmd(),
		daemonCmd(),
		tuiCmd(),
		zensCmd(),
		syncCmd(),
		rotateKeyCmd(),
		bootstrapCmd(),
		relayCmd(),
		pruneCmd(),
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

// dialDaemon connects to the running daemon's gRPC control socket and returns
// the typed client and connection. Callers must close the connection when
// done. An error means the daemon is not running.
func dialDaemon() (driftnodepb.DriftnodeClient, *grpc.ClientConn, error) {
	sock, err := daemonSocketPath()
	if err != nil {
		return nil, nil, err
	}
	return daemon.DialClient(sock)
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
func daemonUnlockCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "unlock",
		Short: "Unlock the daemon's signing key so post/follow/unfollow don't need a passphrase",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if passphrase == "" {
				p, err := readPassphrase("passphrase: ")
				if err != nil {
					return err
				}
				if p == "" {
					return fmt.Errorf("passphrase required")
				}
				passphrase = p
			}
			cl, cc, err := dialDaemon()
			if err != nil {
				return err
			}
			defer cc.Close()
			if _, err := cl.Unlock(context.Background(), &driftnodepb.UnlockReq{Passphrase: passphrase}); err != nil {
				return err
			}
			cmd.Println("key unlocked")
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to unlock the private key (prompted if omitted)")
	return c
}

func daemonLockCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "lock",
		Short: "Clear the daemon's in-memory signing key",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, cc, err := dialDaemon()
			if err != nil {
				return err
			}
			defer cc.Close()
			if _, err := cl.Lock(context.Background(), &driftnodepb.Empty{}); err != nil {
				return err
			}
			cmd.Println("key locked")
			return nil
		},
	}
	addDBFlag(c)
	return c
}
func syncCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "sync",
		Short: "Trigger a sync round",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, cc, err := dialDaemon()
			if err != nil {
				return err
			}
			defer cc.Close()
			_, err = cl.Sync(context.Background(), &driftnodepb.Empty{})
			if err != nil {
				return err
			}
			cmd.Println("sync triggered")
			return nil
		},
	}
	addDBFlag(c)
	return c
}
