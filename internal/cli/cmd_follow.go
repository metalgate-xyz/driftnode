package cli

import (
	"context"
	"errors"
	"fmt"

	"driftnode/internal/core"
	"driftnode/internal/daemon"
	driftnodepb "driftnode/internal/proto/driftnodepb"
	"driftnode/internal/store"

	"github.com/spf13/cobra"
)

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
