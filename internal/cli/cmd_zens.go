package cli

import (
	"context"
	"errors"
	"fmt"

	"driftnode/internal/core"
	"driftnode/internal/daemon"
	driftnodepb "driftnode/internal/proto/driftnodepb"

	"github.com/spf13/cobra"
)

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

func pruneCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "prune",
		Short: "Remove remote PostLog events older than the retention window",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cl, cc, derr := dialDaemon(); derr == nil {
				defer cc.Close()
				resp, err := cl.Prune(context.Background(), &driftnodepb.Empty{})
				if err == nil {
					cmd.Printf("pruned %d remote post events\n", resp.Pruned)
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
			if s.Retention() <= 0 {
				cmd.Println("retention is disabled; nothing to prune")
				return nil
			}
			n, err := s.PruneRemotePosts()
			if err != nil {
				return fmt.Errorf("prune: %w", err)
			}
			cmd.Printf("pruned %d remote post events\n", n)
			return nil
		},
	}
	addDBFlag(c)
	return c
}
