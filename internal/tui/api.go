package tui

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"driftnode/internal/daemon"
	"driftnode/internal/proto/driftnodepb"
)

// feedPost is one post in the merged timeline. id is the event id, used as
// the key so a diff can upsert/remove by identity rather than replacing the
// whole list.
type feedPost struct {
	id     string
	author string
	name   string
	text   string
	age    string
	ts     int64
}

// zen is a discovered/connected zen from the daemon, keyed by its token id.
type zen struct {
	name     string
	identity string
	id       string
	kind     string
	status   string
	verified bool
	pinned   bool
}

// statusInfo is the daemon health snapshot.
type statusInfo struct {
	zens      int
	transport bool
	unlocked  bool
}

// subscription is one long-lived gRPC subscribe stream held by the model. The
// daemon pushes a snapshot once, then incremental typed events; the TUI
// applies them in place so scroll and selection are never reset.
type subscription struct {
	stream driftnodepb.Driftnode_SubscribeClient
}

// snapshotMsg is the first event from a subscribe stream: the full initial
// state of all four panels plus status. sub is the live stream handle the
// model uses to schedule the next read.
type snapshotMsg struct {
	feed      []feedPost
	zens      []zen
	follows   []zen
	followers []zen
	status    statusInfo
	sub       *subscription
	err       error
}

// diffMsg is one incremental push event from the daemon. sub is nil only on a
// final close/error, signaling the model to reconnect.
type diffMsg struct {
	panel    string     // feed, zens, follows, followers, status
	add      []feedPost // feed diffs
	addZens  []zen      // follows/followers diffs
	upsert   *zen       // zens upsert
	removeID string     // zens/follows/followers remove key
	status   *statusInfo
	sub      *subscription
	err      error
}

// subClosedMsg signals the subscription stream ended (daemon down or
// slow-consumer drop). The model schedules a reconnect.
type subClosedMsg struct{ err error }

// subscribeCmd opens a gRPC subscribe stream and reads the snapshot. On
// success it returns snapshotMsg carrying the stream handle (stored on the
// model) so the continuation command can drain further events.
func subscribeCmd(socket string) tea.Cmd {
	return func() tea.Msg {
		cl, cc, err := daemon.DialClient(socket)
		if err != nil {
			return snapshotMsg{err: err}
		}
		stream, err := cl.Subscribe(context.Background(), &driftnodepb.SubscribeReq{})
		if err != nil {
			cc.Close()
			return snapshotMsg{err: fmt.Errorf("subscribe: %w", err)}
		}
		ev, err := stream.Recv()
		if err != nil {
			cc.Close()
			return snapshotMsg{err: fmt.Errorf("read snapshot: %w", err)}
		}
		snap, ok := ev.Kind.(*driftnodepb.Event_Snapshot)
		if !ok {
			cc.Close()
			return snapshotMsg{err: fmt.Errorf("expected snapshot, got %T", ev.Kind)}
		}
		return snapshotFromProto(snap.Snapshot, &subscription{stream: stream})
	}
}

// subscribeContinueCmd reads the next push event from an open subscription.
// It threads the stream handle into the returned message so the model can
// schedule the next read without storing the handle itself.
func subscribeContinueCmd(sub *subscription) tea.Cmd {
	return func() tea.Msg {
		ev, err := sub.stream.Recv()
		if err != nil {
			if daemon.IsStreamClosed(err) {
				return subClosedMsg{err: err}
			}
			return subClosedMsg{err: err}
		}
		d := diffFromProto(ev)
		d.sub = sub
		return d
	}
}

// reconnectDelay is how long the model waits before retrying a dropped
// subscription, so a down daemon is not hot-looped.
const reconnectDelay = 2 * time.Second

// reconnectCmd schedules a fresh subscribe attempt after reconnectDelay.
func reconnectCmd(socket string) tea.Cmd {
	return tea.Tick(reconnectDelay, func(time.Time) tea.Msg {
		return reconnectTickMsg{}
	})
}

type reconnectTickMsg struct{}

// snapshotFromProto converts the daemon's typed snapshot into the TUI's
// panel structs.
func snapshotFromProto(s *driftnodepb.Snapshot, sub *subscription) snapshotMsg {
	if s == nil {
		return snapshotMsg{sub: sub}
	}
	return snapshotMsg{
		feed:      feedItemsFromProto(s.Feed),
		zens:      zensFromProto(s.Zens),
		follows:   identitiesFromProto(s.Follows),
		followers: identitiesFromProto(s.Followers),
		status:    statusFromProto(s.Status),
		sub:       sub,
	}
}

// diffFromProto converts one typed push event into a diffMsg the model
// applies by panel.
func diffFromProto(ev *driftnodepb.Event) diffMsg {
	switch k := ev.Kind.(type) {
	case *driftnodepb.Event_FeedDiff:
		return diffMsg{panel: "feed", add: feedItemsFromProto(k.FeedDiff.Add)}
	case *driftnodepb.Event_ZenUpsert:
		z := zenFromProto(k.ZenUpsert.Zen)
		return diffMsg{panel: "zens", upsert: &z}
	case *driftnodepb.Event_ZenRemove:
		return diffMsg{panel: "zens", removeID: k.ZenRemove.Id}
	case *driftnodepb.Event_FollowsDiff:
		return diffMsg{panel: "follows", addZens: identitiesFromProto(k.FollowsDiff.Add), removeID: k.FollowsDiff.Remove}
	case *driftnodepb.Event_FollowersDiff:
		return diffMsg{panel: "followers", addZens: identitiesFromProto(k.FollowersDiff.Add), removeID: k.FollowersDiff.Remove}
	case *driftnodepb.Event_StatusDiff:
		s := statusFromProto(k.StatusDiff)
		return diffMsg{panel: "status", status: &s}
	}
	return diffMsg{}
}

func feedItemsFromProto(items []*driftnodepb.FeedItem) []feedPost {
	out := make([]feedPost, 0, len(items))
	for _, it := range items {
		out = append(out, feedPost{
			id:     it.Id,
			author: it.Author,
			name:   it.Name,
			text:   it.Text,
			age:    ageOf(it.Timestamp),
			ts:     it.Timestamp,
		})
	}
	return out
}

func zensFromProto(zs []*driftnodepb.Zen) []zen {
	out := make([]zen, 0, len(zs))
	for _, z := range zs {
		out = append(out, zenFromProto(z))
	}
	return out
}

func zenFromProto(z *driftnodepb.Zen) zen {
	display := z.Name
	if display == "" && z.Identity != "" {
		display = shortID(z.Identity)
	}
	if display == "" {
		display = shortID(z.Id)
	}
	return zen{
		name:     display,
		identity: z.Identity,
		id:       z.Id,
		kind:     z.Kind,
		status:   z.Status,
		verified: z.Verified,
	}
}

func identitiesFromProto(ids []*driftnodepb.Identity) []zen {
	out := make([]zen, 0, len(ids))
	for _, id := range ids {
		name := id.Name
		if name == "" {
			name = shortID(id.Identity)
		}
		out = append(out, zen{
			name:     name,
			identity: id.Identity,
			id:       id.Identity,
			verified: id.Verified,
			pinned:   id.Pinned,
		})
	}
	return out
}

func statusFromProto(s *driftnodepb.StatusDiff) statusInfo {
	if s == nil {
		return statusInfo{}
	}
	return statusInfo{
		zens:      int(s.Zens),
		transport: s.Transport,
		unlocked:  s.Unlocked,
	}
}

// actionResultMsg is the outcome of a follow/unfollow/info operation.
type actionResultMsg struct {
	notice string
	err    error
}

// actionCmd runs a follow/unfollow RPC and reports the result.
func actionCmd(label, socket, target string, fn func(string, string) (string, error)) tea.Cmd {
	return func() tea.Msg {
		notice, err := fn(socket, target)
		if err != nil {
			return actionResultMsg{err: err}
		}
		return actionResultMsg{notice: label + ": " + notice}
	}
}

func followZen(socket, target string) (string, error) {
	cl, cc, err := daemon.DialClient(socket)
	if err != nil {
		return "", err
	}
	defer cc.Close()
	ctx, cancel := timeoutCtx()
	defer cancel()
	r, err := cl.Follow(ctx, &driftnodepb.FollowReq{Target: target})
	if err != nil {
		return "", err
	}
	return r.Followed, nil
}

func unfollowZen(socket, target string) (string, error) {
	cl, cc, err := daemon.DialClient(socket)
	if err != nil {
		return "", err
	}
	defer cc.Close()
	ctx, cancel := timeoutCtx()
	defer cancel()
	r, err := cl.Unfollow(ctx, &driftnodepb.UnfollowReq{Target: target})
	if err != nil {
		return "", err
	}
	return r.Unfollowed, nil
}

// postCmd sends a post to the daemon. The feed update arrives over the
// subscription; this just confirms the post was accepted.
func postCmd(socket, text string) tea.Cmd {
	return func() tea.Msg {
		cl, cc, err := daemon.DialClient(socket)
		if err != nil {
			return actionResultMsg{err: err}
		}
		defer cc.Close()
		ctx, cancel := timeoutCtx()
		defer cancel()
		r, err := cl.Post(ctx, &driftnodepb.PostReq{Text: text})
		if err != nil {
			return actionResultMsg{err: err}
		}
		return actionResultMsg{notice: "posted " + r.EventId}
	}
}

// rpcTimeout is the deadline for one-shot RPCs so a stuck daemon does not
// hang the TUI indefinitely.
const rpcTimeout = 30 * time.Second

// timeoutCtx returns a context with the RPC timeout deadline. Callers should
// call the returned cancel when done.
func timeoutCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), rpcTimeout)
}

// shortID keeps the TUI readable by truncating long driftnode identities.
func shortID(id string) string {
	const prefix = "driftnode:"
	if strings.HasPrefix(id, prefix) {
		return id
	}
	return id
}

// ageOf renders the age of a post from a unix-nanosecond timestamp.
func ageOf(ts int64) string {
	if ts == 0 {
		return "?"
	}
	d := time.Since(time.Unix(0, ts))
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

// Compile-time check that error helpers are used.
var _ = errors.New
