package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"driftnode/internal/core"
	"driftnode/internal/daemon"
	driftnodepb "driftnode/internal/proto/driftnodepb"
	"driftnode/internal/store"

	"github.com/spf13/cobra"
)

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
				exists, err := s.EventExists(pid)
				if err != nil {
					return err
				}
				if !exists {
					return fmt.Errorf("post not found locally")
				}
				post.ParentID = pid
			}
			ch, err := s.EnqueuePostEvent(kp, core.PostLog, core.Event{
				Kind: core.KindPost,
				Post: post,
			})
			if err != nil && !errors.Is(err, store.ErrHeld) {
				return err
			}
			res := <-ch
			if res.Err != nil {
				return res.Err
			}
			cmd.Println(res.ID)
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
			exists, err := s.EventExists(target)
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("post not found locally")
			}
			ch, err := s.EnqueuePostEvent(kp, core.PostLog, core.Event{
				Kind: core.KindLike,
				Like: &core.Like{TargetID: target},
			})
			if err != nil && !errors.Is(err, store.ErrHeld) {
				return err
			}
			res := <-ch
			if res.Err != nil {
				return res.Err
			}
			cmd.Println(res.ID)
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
	allPosts, err := s.AllPostsOneTx()
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
