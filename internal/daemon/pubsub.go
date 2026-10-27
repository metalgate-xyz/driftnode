package daemon

import "driftnode/internal/proto/driftnodepb"

// subscriber is one open subscribe stream. The daemon pushes typed events
// onto events; if events fills (slow TUI), done is closed to drop the
// subscriber, and its pump exits so the TUI reconnects.
type subscriber struct {
	events chan *driftnodepb.Event
	done   chan struct{}
}

// newSubscriber constructs a subscriber with the standard buffer size.
func newSubscriber() *subscriber {
	return &subscriber{
		events: make(chan *driftnodepb.Event, subscribeQueue),
		done:   make(chan struct{}),
	}
}

// broadcast sends ev to every subscriber without blocking the daemon. A
// subscriber whose queue is full is dropped (its done channel is closed so
// its pump exits and the stream is torn down).
func (d *Daemon) broadcast(ev *driftnodepb.Event) {
	d.mu.Lock()
	subs := make([]*subscriber, 0, len(d.subs))
	for _, s := range d.subs {
		subs = append(subs, s)
	}
	d.mu.Unlock()
	for _, s := range subs {
		select {
		case s.events <- ev:
		default:
			// Queue full: drop the slow subscriber.
			select {
			case <-s.done:
			default:
				close(s.done)
			}
		}
	}
}

// emitZenUpsert pushes a zens upsert for one zen, enriching it with the
// authoritative name/identity/verified state from the crawl cache. The
// caller must pass a value copied under d.mu so a concurrent setZenStatus
// cannot race the enrichment reads.
func (d *Daemon) emitZenUpsert(z zenInfo) {
	d.broadcast(d.zenDiffEvent(z))
}

// zenDiffEvent builds a zens upsert event for one zen, enriched with the
// name/identity/verified state bound to its token. The enrichment mirrors
// the zens RPC so a push carries the same fields a poll would.
func (d *Daemon) zenDiffEvent(z zenInfo) *driftnodepb.Event {
	zen := d.zenToProto(z)
	return &driftnodepb.Event{Kind: &driftnodepb.Event_ZenUpsert{ZenUpsert: &driftnodepb.ZenUpsert{Zen: zen}}}
}

// emitZenRemove pushes a zens removal by token.
func (d *Daemon) emitZenRemove(id string) {
	d.broadcast(&driftnodepb.Event{Kind: &driftnodepb.Event_ZenRemove{ZenRemove: &driftnodepb.ZenRemove{Id: id}}})
}

// emitFollowsAdd pushes a follows addition.
func (d *Daemon) emitFollowsAdd(entry *driftnodepb.Identity) {
	d.broadcast(&driftnodepb.Event{Kind: &driftnodepb.Event_FollowsDiff{FollowsDiff: &driftnodepb.FollowsDiff{Add: []*driftnodepb.Identity{entry}}}})
}

// emitFollowsRemove pushes a follows removal by identity.
func (d *Daemon) emitFollowsRemove(identity string) {
	d.broadcast(&driftnodepb.Event{Kind: &driftnodepb.Event_FollowsDiff{FollowsDiff: &driftnodepb.FollowsDiff{Remove: identity}}})
}

// emitFollowersAdd pushes a followers addition.
func (d *Daemon) emitFollowersAdd(entry *driftnodepb.Identity) {
	d.broadcast(&driftnodepb.Event{Kind: &driftnodepb.Event_FollowersDiff{FollowersDiff: &driftnodepb.FollowersDiff{Add: []*driftnodepb.Identity{entry}}}})
}

// emitFeedAdd pushes a feed addition (newly merged posts, newest-first).
func (d *Daemon) emitFeedAdd(items []feedItem) {
	if len(items) == 0 {
		return
	}
	add := make([]*driftnodepb.FeedItem, 0, len(items))
	for _, it := range items {
		add = append(add, feedItemToProto(it))
	}
	d.broadcast(&driftnodepb.Event{Kind: &driftnodepb.Event_FeedDiff{FeedDiff: &driftnodepb.FeedDiff{Add: add}}})
}

// emitStatus pushes a status snapshot.
func (d *Daemon) emitStatus() {
	d.mu.Lock()
	tcUp := d.tcListener != nil
	unlocked := d.unlocked != nil
	zenCount := len(d.zens)
	d.mu.Unlock()
	d.broadcast(&driftnodepb.Event{Kind: &driftnodepb.Event_StatusDiff{StatusDiff: &driftnodepb.StatusDiff{
		Running:   true,
		Zens:      int32(zenCount),
		Transport: tcUp,
		Unlocked:  unlocked,
	}}})
}

// addSubscriber registers a subscriber for push events.
func (d *Daemon) addSubscriber(s *subscriber) {
	d.mu.Lock()
	d.subs = append(d.subs, s)
	d.mu.Unlock()
}

// removeSubscriber unregisters a subscriber.
func (d *Daemon) removeSubscriber(s *subscriber) {
	d.mu.Lock()
	for i, sub := range d.subs {
		if sub == s {
			d.subs = append(d.subs[:i], d.subs[i+1:]...)
			break
		}
	}
	d.mu.Unlock()
}
