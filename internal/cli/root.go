// Package cli implements the offline driftnode CLI commands (Phase 0,
// step 2): init, whoami, post, feed, follow/unfollow, key export/import, and
// backup export/import. All commands operate directly on the local bbolt
// store with no daemon or network.
package cli

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	bootstrappkg "driftnode/internal/bootstrap"
	"driftnode/internal/core"
	"driftnode/internal/daemon"
	p2p "driftnode/internal/net"
	"driftnode/internal/store"
	"driftnode/internal/tui"

	"github.com/spf13/cobra"
	"golang.org/x/term"
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
		Short: "Print the identity's public key and current profile and detail",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// When the daemon is running, ask it for the full view; it
			// holds the store lock and the same projection.
			sock, err := daemonSocketPath()
			if err == nil {
				if resp, err := daemon.SendRequest(sock, "whoami", nil); err == nil {
					if m, ok := resp.Result.(map[string]any); ok {
						printWhoami(cmd, m)
						return nil
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
			out := map[string]any{"identity": id.String()}
			if tk, ok, _ := s.TransportKey(); ok {
				if addr, err := p2p.AddrFromKeyBytes(tk); err == nil && addr != "" {
					out["token"] = addr
					out["stable"] = true
				}
			}
			profEvents, _ := s.OwnEvents(core.ProfileLog)
			if prof := core.NewLog(profEvents).Profile(); prof != nil {
				if prof.DisplayName != "" {
					out["display_name"] = prof.DisplayName
				}
				if prof.AvatarHash != nil && !prof.AvatarHash.IsZero() {
					out["has_avatar"] = true
				}
			}
			detEvents, _ := s.OwnEvents(core.DetailLog)
			if det := core.NewLog(detEvents).Detail(); det != nil {
				if det.Bio != "" {
					out["bio"] = det.Bio
				}
				if det.FirstName != "" {
					out["first_name"] = det.FirstName
				}
				if det.LastName != "" {
					out["last_name"] = det.LastName
				}
				if det.Location != "" {
					out["location"] = det.Location
				}
			}
			printWhoami(cmd, out)
			return nil
		},
	}
	addDBFlag(c)
	return c
}

// printWhoami renders the whoami map: identity, then profile fields, then
// detail fields if set. Fields that are absent are omitted.
func printWhoami(cmd *cobra.Command, m map[string]any) {
	cmd.Println(m["identity"])
	if token, ok := m["token"].(string); ok && token != "" {
		if stable, _ := m["stable"].(bool); stable {
			cmd.Printf("address: %s (stable)\n", token)
		} else {
			cmd.Printf("address: %s (ephemeral)\n", token)
		}
	}
	if name, ok := m["display_name"].(string); ok && name != "" {
		cmd.Printf("zen name: %s\n", name)
	}
	if hasAvatar, _ := m["has_avatar"].(bool); hasAvatar {
		cmd.Println("avatar: (present)")
	}
	if bio, ok := m["bio"].(string); ok && bio != "" {
		cmd.Printf("bio: %s\n", bio)
	}
	if fn, ok := m["first_name"].(string); ok && fn != "" {
		cmd.Printf("first name: %s\n", fn)
	}
	if ln, ok := m["last_name"].(string); ok && ln != "" {
		cmd.Printf("last name: %s\n", ln)
	}
	if loc, ok := m["location"].(string); ok && loc != "" {
		cmd.Printf("location: %s\n", loc)
	}
}

func postCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "post <text>",
		Short: "Append a signed Post event to the local PostLog",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// When the daemon is running, post via the control socket. The
			// passphrase is optional then: the daemon holds the unlocked key
			// after `driftnode daemon unlock`.
			sock, err := daemonSocketPath()
			if err == nil {
				params := map[string]any{"text": args[0]}
				if passphrase != "" {
					params["passphrase"] = passphrase
				}
				resp, err := daemon.SendRequest(sock, "post", params)
				if err == nil {
					if m, ok := resp.Result.(map[string]any); ok {
						if id, ok := m["event_id"].(string); ok {
							cmd.Println(id)
							return nil
						}
					}
					return fmt.Errorf("unexpected post response: %v", resp.Result)
				}
				if !errors.Is(err, daemon.ErrNotRunning) {
					return err // daemon up but refused the post (e.g. key locked)
				}
			}
			if passphrase == "" {
				return fmt.Errorf("--passphrase is required when the daemon is not running")
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
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to unlock the private key (optional when the daemon is unlocked)")
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
				name, _ := s.DisplayName(p.Author)
				author := name
				if author == "" {
					author = p.Author.String()
					if len(author) > 16 {
						author = author[:16]
					}
				}
				text := p.Event.Post.Text
				if p.Event.Reply != nil {
					text = "(reply) " + text
				}
				cmd.Printf("%s  %s> %s\n", ts, author, text)
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
		author, _ := m["name"].(string)
		if author == "" {
			author = m["author"].(string)
			if len(author) > 16 {
				author = author[:16]
			}
		}
		text := m["text"].(string)
		cmd.Printf("%s  %s> %s\n", core.FormatTime(ts), author, text)
	}
	return nil
}

func followCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "follow <pubkey-or-token>",
		Short: "Follow an identity, or dial and follow a zen by its token",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err == nil {
				params := map[string]any{"target": args[0]}
				if passphrase != "" {
					params["passphrase"] = passphrase
				}
				resp, err := daemon.SendRequest(sock, "follow", params)
				if err == nil {
					if m, ok := resp.Result.(map[string]any); ok {
						if id, ok := m["followed"].(string); ok {
							cmd.Printf("followed %s\n", id)
							return nil
						}
					}
					return fmt.Errorf("unexpected follow response: %v", resp.Result)
				}
				if !errors.Is(err, daemon.ErrNotRunning) {
					return err
				}
			}
			if passphrase == "" {
				return fmt.Errorf("--passphrase is required when the daemon is not running")
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
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to unlock the private key (optional when the daemon is unlocked)")
	return c
}

func unfollowCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "unfollow <pubkey>",
		Short: "Unfollow an identity (append a signed Unfollow event)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err == nil {
				params := map[string]any{"target": args[0]}
				if passphrase != "" {
					params["passphrase"] = passphrase
				}
				resp, err := daemon.SendRequest(sock, "unfollow", params)
				if err == nil {
					if m, ok := resp.Result.(map[string]any); ok {
						if id, ok := m["unfollowed"].(string); ok {
							cmd.Printf("unfollowed %s\n", id)
							return nil
						}
					}
					return fmt.Errorf("unexpected unfollow response: %v", resp.Result)
				}
				if !errors.Is(err, daemon.ErrNotRunning) {
					return err
				}
			}
			if passphrase == "" {
				return fmt.Errorf("--passphrase is required when the daemon is not running")
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
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to unlock the private key (optional when the daemon is unlocked)")
	return c
}

// followsCmd lists the identities this zen currently follows. It tries the
// daemon first (which holds the store lock); if no daemon is running it
// falls back to a direct read of the local bbolt store (section 12.1).
func followsCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "follows",
		Short: "List the identities this zen follows",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if sock, err := daemonSocketPath(); err == nil {
				if resp, err := daemon.SendRequest(sock, "follows", nil); err == nil {
					printIdentityList(cmd, resp.Result, "follows")
					return nil
				}
			}
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			ids, err := s.FollowGraph()
			if err != nil {
				return err
			}
			printIdentityList(cmd, followsResult("follows", ids, s), "follows")
			return nil
		},
	}
	addDBFlag(c)
	return c
}

// followersCmd lists the identities that follow this zen. It tries the
// daemon first; if no daemon is running it falls back to a direct read of
// the local bbolt store (section 12.1).
func followersCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "followers",
		Short: "List the identities that follow this zen",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if sock, err := daemonSocketPath(); err == nil {
				if resp, err := daemon.SendRequest(sock, "followers", nil); err == nil {
					printIdentityList(cmd, resp.Result, "followers")
					return nil
				}
			}
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			ids, err := s.ReceivedFollowers()
			if err != nil {
				return err
			}
			printIdentityList(cmd, followsResult("followers", ids, s), "followers")
			return nil
		},
	}
	addDBFlag(c)
	return c
}

// followsResult builds a result map shaped like the daemon response from a
// slice of identities, resolving display names from the store.
func followsResult(key string, ids []core.Identity, s *store.Store) map[string]any {
	out := make([]any, 0, len(ids))
	for _, id := range ids {
		entry := map[string]any{"identity": id.String()}
		if name, err := s.DisplayName(id); err == nil && name != "" {
			entry["name"] = name
		}
		out = append(out, entry)
	}
	return map[string]any{key: out}
}

// printIdentityList renders the follows/followers response: one line per
// identity, with the zen name shown if known. An empty list prints nothing.
func printIdentityList(cmd *cobra.Command, result any, key string) {
	m, ok := result.(map[string]any)
	if !ok {
		return
	}
	items, _ := m[key].([]any)
	for _, it := range items {
		entry, ok := it.(map[string]any)
		if !ok {
			continue
		}
		id, _ := entry["identity"].(string)
		name, _ := entry["name"].(string)
		if name != "" {
			cmd.Printf("%s\t%s\n", id, name)
		} else {
			cmd.Println(id)
		}
	}
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
	var passphrase string
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
			// The tailcat transport key is stored raw in the local store; in
			// the backup file it is sealed under the same passphrase as the
			// identity key, so the address token survives a device restore.
			tk, hasTK, err := s.TransportKey()
			if err != nil {
				return fmt.Errorf("read transport key: %w", err)
			}
			if hasTK {
				if passphrase == "" {
					return fmt.Errorf("--passphrase is required to encrypt the transport key into the backup")
				}
				enc, err := core.DefaultKeyEncryption().EncryptBytes(tk, []byte(passphrase))
				if err != nil {
					return fmt.Errorf("encrypt transport key: %w", err)
				}
				bd.TransportKey = enc
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
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to encrypt the transport key into the backup")
	return c
}

func backupImportCmd() *cobra.Command {
	var passphrase string
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
			// Restore the transport key so the address token survives the
			// restore; it is sealed under the same passphrase as the key.
			if bd.TransportKey != nil {
				if passphrase == "" {
					return fmt.Errorf("--passphrase is required to decrypt the transport key")
				}
				tk, err := core.DefaultKeyEncryption().DecryptBytes(bd.TransportKey, []byte(passphrase))
				if err != nil {
					return fmt.Errorf("decrypt transport key: %w", err)
				}
				if err := s.PutTransportKey(tk); err != nil {
					return fmt.Errorf("restore transport key: %w", err)
				}
			}
			cmd.Println("imported backup")
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to decrypt the transport key")
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
	var ephemeral bool
	var bootstrapFile string
	var bootstrapKeyFile string
	var idleLockStr string
	var syncConcurrency int
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
			d.SetSyncConcurrency(syncConcurrency)
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
	c.AddCommand(daemonStopCmd(), daemonStatusCmd(), daemonUnlockCmd(), daemonLockCmd())
	c.Flags().BoolVarP(&foreground, "foreground", "f", false, "run in foreground with logs on stdout")
	c.Flags().BoolVar(&ephemeral, "ephemeral", false, "generate a fresh address token each run (do not persist the transport key)")
	c.Flags().StringVar(&bootstrapFile, "bootstrap", "", "path to a signed bootstrap.yaml to load and auto-dial seed zens")
	c.Flags().StringVar(&bootstrapKeyFile, "bootstrap-key", "", "path to a file containing the base64 Ed25519 public key that signed the bootstrap")
	c.Flags().StringVar(&idleLockStr, "idle-lock", "", "auto-lock the signing key after this idle duration (e.g. 5m, 1h); default keeps it unlocked until 'daemon lock' or stop")
	c.Flags().IntVar(&syncConcurrency, "sync-concurrency", 8, "maximum number of zen dials to run in parallel during a sync round")
	return c
}

func daemonUnlockCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "unlock",
		Short: "Unlock the daemon's signing key so post/follow/unfollow don't need a passphrase",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			params := map[string]any{}
			if passphrase != "" {
				params["passphrase"] = passphrase
			} else {
				p, err := readPassphrase("passphrase: ")
				if err != nil {
					return err
				}
				if p == "" {
					return fmt.Errorf("passphrase required")
				}
				params["passphrase"] = p
			}
			if _, err := daemon.SendRequest(sock, "unlock", params); err != nil {
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
			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			if _, err := daemon.SendRequest(sock, "lock", nil); err != nil {
				return err
			}
			cmd.Println("key locked")
			return nil
		},
	}
	addDBFlag(c)
	return c
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
			cmd.Printf("zens: %v\n", result["zens"])
			cmd.Printf("transport: %v\n", result["transport"])
			cmd.Printf("unlocked: %v\n", result["unlocked"])
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
			// The TUI is a thin client of the daemon's control socket; it
			// never opens the store, so it can run alongside the daemon
			// (which holds the bbolt lock).
			whoami, err := daemon.SendRequest(sock, "whoami", nil)
			if err != nil {
				return fmt.Errorf("daemon not running; start it with 'driftnode daemon' first: %w", err)
			}
			m, ok := whoami.Result.(map[string]any)
			if !ok {
				return fmt.Errorf("unexpected whoami response")
			}
			identity, _ := m["identity"].(string)
			if identity == "" {
				return fmt.Errorf("no identity found; run 'driftnode init' first")
			}
			// Unlock the daemon's signing key once, so compose can post
			// without re-prompting. If already unlocked, the daemon keeps
			// the existing key.
			status, _ := daemon.SendRequest(sock, "status", nil)
			if sm, ok := status.Result.(map[string]any); ok && !sm["unlocked"].(bool) {
				passphrase, err := readPassphrase("passphrase: ")
				if err != nil {
					return err
				}
				if _, err := daemon.SendRequest(sock, "unlock", map[string]any{"passphrase": passphrase}); err != nil {
					return err
				}
			}
			return tui.Run(sock, identity)
		},
	}
	addDBFlag(c)
	return c
}

func zensCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "zens",
		Short: "Show your network: connected zens",
	}
	addDBFlagPersistent(c)
	c.AddCommand(zensListCmd(), zensVerifyCmd(), zensUnverifyCmd())
	return c
}

func zensListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "Show currently connected zens",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			resp, err := daemon.SendRequest(sock, "zens", nil)
			if err != nil {
				return err
			}
			zens, ok := resp.Result.([]any)
			if !ok || len(zens) == 0 {
				cmd.Println("(no zens connected)")
				return nil
			}
			for _, p := range zens {
				pm, ok := p.(map[string]any)
				if !ok {
					continue
				}
				identity, _ := pm["identity"].(string)
				if identity == "" {
					identity = "(unknown)"
				}
				mark := " "
				if v, _ := pm["verified"].(bool); v {
					mark = "✓"
				}
				name, _ := pm["name"].(string)
				if name != "" {
					cmd.Printf("%s %s  %s  %s (%s)\n", mark, pm["status"], name, identity, pm["kind"])
				} else {
					cmd.Printf("%s %s  %s (%s)\n", mark, pm["status"], identity, pm["kind"])
				}
			}
			return nil
		},
	}
}

func zensVerifyCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "verify <identity>",
		Short: "Mark an identity as confirmed out-of-band (e.g. compared via Signal)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := core.ParseIdentity(args[0])
			if err != nil {
				return fmt.Errorf("invalid identity: %w", err)
			}
			sock, err := daemonSocketPath()
			if err == nil {
				if _, err := daemon.SendRequest(sock, "verify", map[string]any{"identity": string(id)}); err == nil {
					cmd.Printf("verified %s\n", id)
					return nil
				}
			}
			// Offline: write directly to the store.
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			if err := s.VerifyIdentity(id); err != nil {
				return err
			}
			cmd.Printf("verified %s\n", id)
			return nil
		},
	}
}

func zensUnverifyCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unverify <identity>",
		Short: "Remove an out-of-band confirmation from an identity",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := core.ParseIdentity(args[0])
			if err != nil {
				return fmt.Errorf("invalid identity: %w", err)
			}
			sock, err := daemonSocketPath()
			if err == nil {
				if _, err := daemon.SendRequest(sock, "unverify", map[string]any{"identity": string(id)}); err == nil {
					cmd.Printf("unverified %s\n", id)
					return nil
				}
			}
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			if err := s.UnverifyIdentity(id); err != nil {
				return err
			}
			cmd.Printf("unverified %s\n", id)
			return nil
		},
	}
}

func syncCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "sync",
		Short: "Trigger a sync round",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			_, err = daemon.SendRequest(sock, "sync", nil)
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

func rotateKeyCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "rotate-key",
		Short: "Generate a fresh address token, persist it, and restart the listener",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := daemonSocketPath()
			if err != nil {
				return err
			}
			resp, err := daemon.SendRequest(sock, "rotate-key", nil)
			if err != nil {
				return err
			}
			m, ok := resp.Result.(map[string]any)
			if !ok {
				return fmt.Errorf("unexpected rotate-key response: %v", resp.Result)
			}
			token, _ := m["token"].(string)
			cmd.Println("rotated address token:")
			cmd.Println("address:", token, "(stable)")
			cmd.Println("followers must re-discover your identity to use the new token")
			return nil
		},
	}
	addDBFlag(c)
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
			cmd.Printf("seed_zens: %d\n", len(bf.SeedZens))
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
			cmd.Println("relay role: not advertised")
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

// --- profile and detail commands ---

func profileCmd() *cobra.Command {
	var passphrase string
	var name string
	c := &cobra.Command{
		Use:   "profile",
		Short: "Set the public profile (zen name) shown in the crawl cache",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			sock, err := daemonSocketPath()
			if err == nil {
				params := map[string]any{"name": name}
				if passphrase != "" {
					params["passphrase"] = passphrase
				}
				resp, err := daemon.SendRequest(sock, "profile", params)
				if err == nil {
					if m, ok := resp.Result.(map[string]any); ok {
						if id, ok := m["event_id"].(string); ok {
							cmd.Println(id)
							return nil
						}
					}
					return fmt.Errorf("unexpected profile response: %v", resp.Result)
				}
				if !errors.Is(err, daemon.ErrNotRunning) {
					return err
				}
			}
			if passphrase == "" {
				return fmt.Errorf("--passphrase is required when the daemon is not running")
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
			seq, err := s.OwnEventCount(core.ProfileLog)
			if err != nil {
				return fmt.Errorf("get sequence: %w", err)
			}
			se, err := kp.Sign(core.Event{
				Kind:      core.KindProfile,
				Log:       core.ProfileLog,
				Timestamp: core.Now64(),
				Sequence:  seq + 1,
				Profile:   &core.Profile{DisplayName: name},
			})
			if err != nil {
				return fmt.Errorf("sign: %w", err)
			}
			if err := s.AppendOwnEvent(core.ProfileLog, se); err != nil {
				return fmt.Errorf("append: %w", err)
			}
			id, _ := se.ID()
			cmd.Println(id)
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to unlock the private key (optional when the daemon is unlocked)")
	c.Flags().StringVar(&name, "name", "", "zen name (public, crawled)")
	return c
}

func detailCmd() *cobra.Command {
	var passphrase string
	var bio string
	var firstName string
	var lastName string
	var location string
	c := &cobra.Command{
		Use:   "detail",
		Short: "Set personal metadata (bio, first/last name, location) shown only on direct request",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if bio == "" && firstName == "" && lastName == "" && location == "" {
				return fmt.Errorf("provide at least one of --bio, --first-name, --last-name, --location")
			}
			sock, err := daemonSocketPath()
			if err == nil {
				params := map[string]any{}
				if bio != "" {
					params["bio"] = bio
				}
				if firstName != "" {
					params["first_name"] = firstName
				}
				if lastName != "" {
					params["last_name"] = lastName
				}
				if location != "" {
					params["location"] = location
				}
				if passphrase != "" {
					params["passphrase"] = passphrase
				}
				resp, err := daemon.SendRequest(sock, "detail", params)
				if err == nil {
					if m, ok := resp.Result.(map[string]any); ok {
						if id, ok := m["event_id"].(string); ok {
							cmd.Println(id)
							return nil
						}
					}
					return fmt.Errorf("unexpected detail response: %v", resp.Result)
				}
				if !errors.Is(err, daemon.ErrNotRunning) {
					return err
				}
			}
			if passphrase == "" {
				return fmt.Errorf("--passphrase is required when the daemon is not running")
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
			seq, err := s.OwnEventCount(core.DetailLog)
			if err != nil {
				return fmt.Errorf("get sequence: %w", err)
			}
			se, err := kp.Sign(core.Event{
				Kind:      core.KindDetail,
				Log:       core.DetailLog,
				Timestamp: core.Now64(),
				Sequence:  seq + 1,
				Detail:    &core.Detail{Bio: bio, FirstName: firstName, LastName: lastName, Location: location},
			})
			if err != nil {
				return fmt.Errorf("sign: %w", err)
			}
			if err := s.AppendOwnEvent(core.DetailLog, se); err != nil {
				return fmt.Errorf("append: %w", err)
			}
			id, _ := se.ID()
			cmd.Println(id)
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to unlock the private key (optional when the daemon is unlocked)")
	c.Flags().StringVar(&bio, "bio", "", "bio (shown only on direct request)")
	c.Flags().StringVar(&firstName, "first-name", "", "first name (shown only on direct request)")
	c.Flags().StringVar(&lastName, "last-name", "", "last name (shown only on direct request)")
	c.Flags().StringVar(&location, "location", "", "location (shown only on direct request)")
	return c
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
