// Package cli implements the offline driftnode CLI commands (Phase 0,
// step 2): init, whoami, post, feed, follow/unfollow, key export/import, and
// backup export/import. All commands operate directly on the local bbolt
// store with no daemon or network.
package cli

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
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

	bootstrappkg "driftnode/internal/bootstrap"
	"driftnode/internal/core"
	"driftnode/internal/daemon"
	p2p "driftnode/internal/net"
	"driftnode/internal/proto/driftnodepb"
	"driftnode/internal/store"
	"driftnode/internal/tui"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	"google.golang.org/grpc"
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
			if cl, cc, err := dialDaemon(); err == nil {
				defer cc.Close()
				resp, err := cl.Whoami(context.Background(), &driftnodepb.Empty{})
				if err == nil {
					printWhoami(cmd, resp)
					return nil
				}
				if !errors.Is(err, daemon.ErrNotRunning) {
					return err
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
			out := &driftnodepb.WhoamiResp{Identity: id.String()}
			if tk, ok, _ := s.TransportKey(); ok {
				if addr, err := p2p.AddrFromKeyBytes(tk); err == nil && addr != "" {
					out.Token = addr
					out.Stable = true
				}
			}
			profEvents, _ := s.OwnEvents(core.ProfileLog)
			if prof := core.NewLog(profEvents).Profile(); prof != nil {
				if prof.DisplayName != "" {
					out.DisplayName = prof.DisplayName
				}
				if prof.AvatarHash != nil && !prof.AvatarHash.IsZero() {
					out.HasAvatar = true
				}
			}
			detEvents, _ := s.OwnEvents(core.DetailLog)
			if det := core.NewLog(detEvents).Detail(); det != nil {
				if det.Bio != "" {
					out.Bio = det.Bio
				}
				if det.FirstName != "" {
					out.FirstName = det.FirstName
				}
				if det.LastName != "" {
					out.LastName = det.LastName
				}
				if det.Location != "" {
					out.Location = det.Location
				}
			}
			printWhoami(cmd, out)
			return nil
		},
	}
	addDBFlag(c)
	return c
}

// printWhoami renders the identity and optional profile fields from the
// daemon's whoami response.
func printWhoami(cmd *cobra.Command, r *driftnodepb.WhoamiResp) {
	cmd.Println(r.Identity)
	if r.Token != "" {
		if r.Stable {
			cmd.Printf("address: %s (stable)\n", r.Token)
		} else {
			cmd.Printf("address: %s (ephemeral)\n", r.Token)
		}
	}
	if r.DisplayName != "" {
		cmd.Printf("zen name: %s\n", r.DisplayName)
	}
	if r.HasAvatar {
		cmd.Println("avatar: (present)")
	}
	if r.Bio != "" {
		cmd.Printf("bio: %s\n", r.Bio)
	}
	if r.FirstName != "" {
		cmd.Printf("first name: %s\n", r.FirstName)
	}
	if r.LastName != "" {
		cmd.Printf("last name: %s\n", r.LastName)
	}
	if r.Location != "" {
		cmd.Printf("location: %s\n", r.Location)
	}
}

func postCmd() *cobra.Command {
	var passphrase string
	var parentID string
	c := &cobra.Command{
		Use:   "post <text>",
		Short: "Append a signed Post event to the local PostLog",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// When the daemon is running, post via the control socket. The
			// passphrase is optional then: the daemon holds the unlocked key
			// after `driftnode daemon unlock`.
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				resp, err := cl.Post(context.Background(), &driftnodepb.PostReq{Text: args[0], Passphrase: passphrase, ParentId: parentID})
				if err == nil {
					cmd.Println(resp.EventId)
					return nil
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
			post := &core.Post{Text: args[0]}
			if parentID != "" {
				pid, err := core.ParseEventID(parentID)
				if err != nil {
					return fmt.Errorf("parent id: %w", err)
				}
				post.ParentID = pid
			}
			_, id, err := s.SignAndAppend(kp, core.PostLog, core.Event{
				Kind: core.KindPost,
				Post: post,
			})
			if err != nil {
				return err
			}
			cmd.Println(id)
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to unlock the private key (optional when the daemon is unlocked)")
	c.Flags().StringVar(&parentID, "parent", "", "event ID of the post to reply to (creates a reply)")
	return c
}

func likeCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "like <post-id>",
		Short: "Like a post (append a signed Like event to the local PostLog)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				resp, err := cl.Like(context.Background(), &driftnodepb.LikeReq{PostId: args[0], Passphrase: passphrase})
				if err == nil {
					cmd.Println(resp.EventId)
					return nil
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
			target, err := core.ParseEventID(args[0])
			if err != nil {
				return fmt.Errorf("post id: %w", err)
			}
			if target.IsZero() {
				return fmt.Errorf("post id required")
			}
			_, id, err := s.SignAndAppend(kp, core.PostLog, core.Event{
				Kind: core.KindLike,
				Like: &core.Like{TargetID: target},
			})
			if err != nil {
				return err
			}
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
	var mine bool
	var allLikes bool
	c := &cobra.Command{
		Use:   "feed",
		Short: "Print the merged timeline from local state (no network)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// When the daemon is running (it holds the store lock), read the
			// feed via the subscribe snapshot (page 0) plus FeedPage for older
			// posts until the limit is reached or the feed is exhausted.
			// Otherwise open the store directly for offline use.
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				return feedOnline(cmd, cl, limit, mine, allLikes)
			}
			if allLikes {
				return fmt.Errorf("--all-likes requires the daemon")
			}
			return feedOffline(cmd, limit, mine)
		},
	}
	addDBFlag(c)
	c.Flags().IntVarP(&limit, "limit", "n", 0, "maximum number of posts to show (0 = all)")
	c.Flags().BoolVar(&mine, "mine", false, "show only your own posts")
	c.Flags().BoolVar(&allLikes, "all-likes", false, "fetch likes from connected zens for your own posts (requires daemon)")
	return c
}

// feedOnline reads the feed from the running daemon via FeedPage, newest
// first, until limit is reached or the feed is exhausted. all_likes is
// passed to FeedPage so the daemon fetches likes from connected zens for the
// user's own posts in each page. Each page is printed as it arrives so a
// large feed streams to the terminal instead of buffering the whole history
// before the first line.
func feedOnline(cmd *cobra.Command, cl driftnodepb.DriftnodeClient, limit int, mine, allLikes bool) error {
	page := 0
	shown := 0
	for {
		resp, err := cl.FeedPage(context.Background(), &driftnodepb.FeedPageReq{Page: int32(page), Mine: mine, AllLikes: allLikes})
		if err != nil {
			return fmt.Errorf("feed page %d: %w", page, err)
		}
		items := resp.Items
		if limit > 0 && shown+len(items) > limit {
			items = items[:limit-shown]
		}
		printFeedItems(cmd, items)
		shown += len(items)
		if !resp.HasMore {
			break
		}
		if limit > 0 && shown >= limit {
			break
		}
		page++
	}
	return nil
}

// feedOffline reads the feed directly from the local store when the daemon
// is not running.
func feedOffline(cmd *cobra.Command, limit int, mine bool) error {
	s, err := openStoreAt(dbPath)
	if err != nil {
		return err
	}
	defer s.Close()
	ownID, _, _ := s.Identity()
	allPosts, err := s.AllPosts()
	if err != nil {
		return fmt.Errorf("read posts: %w", err)
	}
	log := core.NewLog(allPosts)
	posts := log.Posts()
	if limit > 0 && len(posts) > limit {
		posts = posts[:limit]
	}
	// Build like counts per post from Like events in the held PostLogs.
	postLikes := make(map[core.EventID][]string)
	for _, se := range allPosts {
		if se.Event.Kind != core.KindLike || se.Event.Like == nil {
			continue
		}
		likerName, _ := s.DisplayName(se.Author)
		if likerName == "" {
			likerName = se.Author.String()
		}
		postLikes[se.Event.Like.TargetID] = append(postLikes[se.Event.Like.TargetID], likerName)
	}
	for _, p := range posts {
		if mine && p.Author != ownID {
			continue
		}
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
		if p.Event.Post != nil && !p.Event.Post.ParentID.IsZero() {
			text = "(reply) " + text
		}
		line := fmt.Sprintf("%s  %s> %s", ts, author, text)
		pid, _ := p.ID()
		if likers, ok := postLikes[pid]; ok && len(likers) > 0 {
			line += fmt.Sprintf("  %d %s: %s", len(likers), core.GlyphLike, strings.Join(likers, ", "))
		}
		line += "  [" + pid.String() + "]"
		cmd.Println(line)
	}
	return nil
}

// printFeedItems prints proto feed items with like counts and liker names.
func printFeedItems(cmd *cobra.Command, items []*driftnodepb.FeedItem) {
	for _, item := range items {
		author := item.Name
		if author == "" {
			author = item.Author
			if len(author) > 16 {
				author = author[:16]
			}
		}
		line := fmt.Sprintf("%s  %s> %s", core.FormatTime(item.Timestamp), author, item.Text)
		if item.LikeCount > 0 {
			line += fmt.Sprintf("  %d %s: %s", item.LikeCount, core.GlyphLike, strings.Join(item.Likers, ", "))
		}
		if item.Id != "" {
			line += "  [" + item.Id + "]"
		}
		cmd.Println(line)
	}
}

// feedPageSize is the page size the daemon serves via FeedPage. It must
// match the daemon's feedPageLimit.
const feedPageSize = 200

// subscribeSnapshot opens a subscribe stream, reads the initial snapshot,
// and returns it. Used by commands that need live state panels (follows,
// followers, zens), not by the feed command.
func subscribeSnapshot(cl driftnodepb.DriftnodeClient) (*driftnodepb.Snapshot, error) {
	stream, err := cl.Subscribe(context.Background(), &driftnodepb.SubscribeReq{})
	if err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}
	ev, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	s, ok := ev.Kind.(*driftnodepb.Event_Snapshot)
	if !ok {
		return nil, fmt.Errorf("expected snapshot, got %T", ev.Kind)
	}
	return s.Snapshot, nil
}

func followCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "follow <pubkey-or-token>",
		Short: "Follow an identity, or dial and follow a zen by its token",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				resp, err := cl.Follow(context.Background(), &driftnodepb.FollowReq{Target: args[0], Passphrase: passphrase})
				if err == nil {
					cmd.Printf("followed %s\n", resp.Followed)
					return nil
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
			target, err := core.ResolvePubkey(args[0])
			if err != nil {
				return err
			}
			if _, _, id, err := s.Follow(kp, target); err != nil {
				return err
			} else {
				cmd.Printf("followed %s\n", id)
			}
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
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				resp, err := cl.Unfollow(context.Background(), &driftnodepb.UnfollowReq{Target: args[0], Passphrase: passphrase})
				if err == nil {
					cmd.Printf("unfollowed %s\n", resp.Unfollowed)
					return nil
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
			target, err := core.ResolvePubkey(args[0])
			if err != nil {
				return err
			}
			if _, _, id, err := s.Unfollow(kp, target); err != nil {
				return err
			} else {
				cmd.Printf("unfollowed %s\n", id)
			}
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
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				snap, err := subscribeSnapshot(cl)
				if err == nil {
					printIdentities(cmd, snap.GetFollows())
					return nil
				}
				if !errors.Is(err, daemon.ErrNotRunning) {
					return err
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
			printIdentities(cmd, identitiesFromStore(ids, s, true))
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
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				snap, err := subscribeSnapshot(cl)
				if err == nil {
					printIdentities(cmd, snap.GetFollowers())
					return nil
				}
				if !errors.Is(err, daemon.ErrNotRunning) {
					return err
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
			printIdentities(cmd, identitiesFromStore(ids, s, false))
			return nil
		},
	}
	addDBFlag(c)
	return c
}

// identitiesFromStore builds an identity list from a slice of core identities,
// resolving display names and trust/priority state from the store. Used by
// the offline fallback path. withPinned controls whether the pin flag is
// projected: it applies only to follows, not followers.
func identitiesFromStore(ids []core.Identity, s *store.Store, withPinned bool) []*driftnodepb.Identity {
	out := make([]*driftnodepb.Identity, 0, len(ids))
	for _, id := range ids {
		entry := &driftnodepb.Identity{Identity: id.String()}
		if name, err := s.DisplayName(id); err == nil && name != "" {
			entry.Name = name
		}
		if v, _ := s.IsVerified(id); v {
			entry.Verified = true
		}
		if withPinned {
			if p, _ := s.IsPinned(id); p {
				entry.Pinned = true
			}
		}
		out = append(out, entry)
	}
	return out
}

// printIdentities renders follows/followers from a daemon RPC response,
// with a verified marker and, for follows, a pin marker.
func printIdentities(cmd *cobra.Command, items []*driftnodepb.Identity) {
	for _, entry := range items {
		marks := ""
		if entry.Verified {
			marks += core.GlyphVerified + " "
		}
		if entry.Pinned {
			marks += core.GlyphPin + " "
		}
		if entry.Name != "" {
			cmd.Printf("%s%s\t%s\n", marks, entry.Identity, entry.Name)
		} else {
			cmd.Printf("%s%s\n", marks, entry.Identity)
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

// --- daemon and network commands ---

func daemonCmd() *cobra.Command {
	var detached bool
	var detachedChild bool
	var logFile string
	var ephemeral bool
	var bootstrapFile string
	var bootstrapKeyFile string
	var idleLockStr string
	var syncConcurrency int
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
	c.Flags().IntVar(&syncConcurrency, "sync-concurrency", 8, "maximum number of zen dials to run in parallel during a sync round")
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
	for _, f := range []string{"ephemeral", "bootstrap", "bootstrap-key", "idle-lock", "sync-concurrency"} {
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
	var syncConcurrency int
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
	c.Flags().IntVar(&syncConcurrency, "sync-concurrency", 8, "maximum number of zen dials to run in parallel during a sync round")
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

func zensCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "zens",
		Short: "Show your network: connected zens",
	}
	addDBFlagPersistent(c)
	c.AddCommand(zensListCmd(), zensVerifyCmd(), zensUnverifyCmd(), zensPinCmd(), zensUnpinCmd())
	return c
}

func zensListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "Show currently connected zens",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, cc, err := dialDaemon()
			if err != nil {
				return err
			}
			defer cc.Close()
			snap, err := subscribeSnapshot(cl)
			if err != nil {
				return err
			}
			zens := snap.GetZens()
			if len(zens) == 0 {
				cmd.Println("(no zens connected)")
				return nil
			}
			for _, z := range zens {
				identity := z.Identity
				if identity == "" {
					identity = "(unknown)"
				}
				mark := " "
				if z.Verified {
					mark = core.GlyphVerified
				}
				if z.Name != "" {
					cmd.Printf("%s %s  %s  %s (%s)\n", mark, z.Status, z.Name, identity, z.Kind)
				} else {
					cmd.Printf("%s %s  %s (%s)\n", mark, z.Status, identity, z.Kind)
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
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				_, err := cl.Verify(context.Background(), &driftnodepb.IdentityReq{Identity: string(id)})
				if err == nil {
					cmd.Printf("verified %s\n", id)
					return nil
				}
				if !errors.Is(err, daemon.ErrNotRunning) {
					return err
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
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				_, err := cl.Unverify(context.Background(), &driftnodepb.IdentityReq{Identity: string(id)})
				if err == nil {
					cmd.Printf("unverified %s\n", id)
					return nil
				}
				if !errors.Is(err, daemon.ErrNotRunning) {
					return err
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

func zensPinCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "pin <identity>",
		Short: "Pin a followed identity for top sync priority (local state, never synced)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := core.ParseIdentity(args[0])
			if err != nil {
				return fmt.Errorf("invalid identity: %w", err)
			}
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				_, err := cl.Pin(context.Background(), &driftnodepb.IdentityReq{Identity: string(id)})
				if err == nil {
					cmd.Printf("pinned %s\n", id)
					return nil
				}
				if !errors.Is(err, daemon.ErrNotRunning) {
					return err
				}
			}
			// Offline: write directly to the store.
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			if err := s.PinIdentity(id); err != nil {
				return err
			}
			cmd.Printf("pinned %s\n", id)
			return nil
		},
	}
}

func zensUnpinCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unpin <identity>",
		Short: "Remove a pin from an identity (also removed automatically on unfollow)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := core.ParseIdentity(args[0])
			if err != nil {
				return fmt.Errorf("invalid identity: %w", err)
			}
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				_, err := cl.Unpin(context.Background(), &driftnodepb.IdentityReq{Identity: string(id)})
				if err == nil {
					cmd.Printf("unpinned %s\n", id)
					return nil
				}
				if !errors.Is(err, daemon.ErrNotRunning) {
					return err
				}
			}
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			if err := s.UnpinIdentity(id); err != nil {
				return err
			}
			cmd.Printf("unpinned %s\n", id)
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

func rotateKeyCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "rotate-key",
		Short: "Generate a fresh address token, persist it, and restart the listener",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, cc, err := dialDaemon()
			if err != nil {
				return err
			}
			defer cc.Close()
			resp, err := cl.RotateKey(context.Background(), &driftnodepb.Empty{})
			if err != nil {
				return err
			}
			cmd.Println("rotated address token:")
			cmd.Println("address:", resp.Token, "(stable)")
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
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				resp, err := cl.Profile(context.Background(), &driftnodepb.ProfileReq{Name: name, Passphrase: passphrase})
				if err == nil {
					cmd.Println(resp.Status)
					return nil
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
			_, id, err := s.SignAndAppend(kp, core.ProfileLog, core.Event{
				Kind:    core.KindProfile,
				Profile: &core.Profile{DisplayName: name},
			})
			if err != nil {
				return err
			}
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
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				resp, err := cl.Detail(context.Background(), &driftnodepb.DetailReq{
					Bio: bio, FirstName: firstName, LastName: lastName, Location: location, Passphrase: passphrase,
				})
				if err == nil {
					cmd.Println(resp.Status)
					return nil
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
			_, id, err := s.SignAndAppend(kp, core.DetailLog, core.Event{
				Kind:   core.KindDetail,
				Detail: &core.Detail{Bio: bio, FirstName: firstName, LastName: lastName, Location: location},
			})
			if err != nil {
				return err
			}
			cmd.Println(id)
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
