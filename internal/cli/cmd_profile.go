package cli

import (
	"context"
	"errors"
	"fmt"

	"driftnode/internal/core"
	"driftnode/internal/daemon"
	p2p "driftnode/internal/net"
	driftnodepb "driftnode/internal/proto/driftnodepb"

	"github.com/spf13/cobra"
)

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

// pruneCmd removes stale remote PostLog events older than the retention
// window from the local store. Own PostLog events are retained in full.
// When the daemon is running it prunes via RPC (so the TUI and a running
// daemon share one path); otherwise it opens the store directly.
