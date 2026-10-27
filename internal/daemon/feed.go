package daemon

import (
	"driftnode/internal/core"
	"driftnode/internal/proto/driftnodepb"
)

// populateLikes computes the liker set for each feed item from Like events
// held locally (own PostLog plus synced PostLogs of followed identities).
// Likes are observational: the count is "how many Like events this zen has
// observed," never a global truth (section 7.2).
func (d *Daemon) populateLikes(items []feedItem) {
	if len(items) == 0 {
		return
	}
	ids := make([]core.EventID, len(items))
	idToIdx := make(map[core.EventID]int, len(items))
	for i, it := range items {
		id, err := core.ParseEventID(it.ID)
		if err != nil {
			continue
		}
		ids[i] = id
		idToIdx[id] = i
	}
	likes, err := d.store.LikesForPosts(ids)
	if err != nil {
		return
	}
	for _, se := range likes {
		if se.Event.Like == nil {
			continue
		}
		idx, ok := idToIdx[se.Event.Like.TargetID]
		if !ok {
			continue
		}
		items[idx].LikeCount++
		likerName := d.displayName(se.Author)
		if likerName == "" {
			likerName = se.Author.String()
		}
		items[idx].Likers = append(items[idx].Likers, likerName)
	}
}

// cachedPosts returns the merged timeline sorted newest-first, rebuilding it
// from disk only when the feed generation has changed since the last build.
// A large feed (millions of posts) otherwise reloads and re-sorts on every
// subscribe snapshot, freezing the TUI for seconds.
func (d *Daemon) cachedPosts() []core.SignedEvent {
	d.feedMu.Lock()
	gen := d.feedGen
	cached := d.feedCache
	d.feedMu.Unlock()
	if cached != nil {
		return cached
	}
	// Cache miss: rebuild. feedGen is bumped on any post mutation, so a
	// concurrent bump is caught on the next call (we rebuild under the lock
	// and store the result for the current gen).
	d.feedMu.Lock()
	defer d.feedMu.Unlock()
	if d.feedCache != nil && d.feedGen == gen {
		return d.feedCache
	}
	allPosts, err := d.store.AllPostsOneTx()
	if err != nil {
		d.logger.Info("cachedPosts AllPostsOneTx error", "err", err)
		return nil
	}
	posts := core.NewLog(allPosts).Posts()
	d.feedCache = posts
	return posts
}

// invalidateFeed bumps the feed generation so the next cachedPosts call
// rebuilds from disk. Called when a post is created or synced.
func (d *Daemon) invalidateFeed() {
	d.feedMu.Lock()
	d.feedGen++
	d.feedCache = nil
	d.feedMu.Unlock()
}

// invalidateNames drops the name cache so the next DisplayName lookup
// re-reads from disk. Called when a profile is written.
func (d *Daemon) invalidateNames() {
	d.feedMu.Lock()
	d.nameCache = make(map[core.Identity]string)
	d.feedMu.Unlock()
}

// displayName returns the cached display name for an identity, populating
// the cache on first access. The per-identity projection over the
// ProfileLog is expensive at scale (one bbolt scan per item), so memoizing
// keeps the snapshot and feed diffs from re-running it per row.
func (d *Daemon) displayName(id core.Identity) string {
	d.feedMu.Lock()
	if d.nameCache == nil {
		d.nameCache = make(map[core.Identity]string)
	}
	if name, ok := d.nameCache[id]; ok {
		d.feedMu.Unlock()
		return name
	}
	d.feedMu.Unlock()
	name, _ := d.store.DisplayName(id)
	d.feedMu.Lock()
	d.nameCache[id] = name
	d.feedMu.Unlock()
	return name
}

// followEntries returns the identities this zen follows, with display names,
// for the subscribe snapshot.
func (d *Daemon) followEntries() []*driftnodepb.Identity {
	ids, err := d.store.FollowGraph()
	if err != nil {
		return nil
	}
	out := make([]*driftnodepb.Identity, 0, len(ids))
	for _, id := range ids {
		entry := &driftnodepb.Identity{Identity: id.String()}
		if name := d.displayName(id); name != "" {
			entry.Name = name
		}
		if v, _ := d.store.IsVerified(id); v {
			entry.Verified = true
		}
		if p, _ := d.store.IsPinned(id); p {
			entry.Pinned = true
		}
		out = append(out, entry)
	}
	return out
}

// followerEntries returns the identities that follow this zen, with display
// names, for the subscribe snapshot.
func (d *Daemon) followerEntries() []*driftnodepb.Identity {
	ids, err := d.store.ReceivedFollowers()
	if err != nil {
		return nil
	}
	out := make([]*driftnodepb.Identity, 0, len(ids))
	for _, id := range ids {
		entry := &driftnodepb.Identity{Identity: id.String()}
		if name := d.displayName(id); name != "" {
			entry.Name = name
		}
		if v, _ := d.store.IsVerified(id); v {
			entry.Verified = true
		}
		out = append(out, entry)
	}
	return out
}

// feedItem is one post in the merged feed, serialized to the CLI.
type feedItem struct {
	ID        string   `json:"id"`
	Timestamp int64    `json:"timestamp"`
	Author    string   `json:"author"`
	Name      string   `json:"name"`
	Text      string   `json:"text"`
	LikeCount int      `json:"like_count"`
	Likers    []string `json:"likers"`
}

// handleFeedPage returns one page of the merged timeline, newest-first. Page
// 0 is the snapshot. hasMore reports whether older posts remain. When
// allLikes is set, the daemon fetches likes from connected zens for the
// user's own posts in the page before populating like counts.
func (d *Daemon) handleFeedPage(page int, mine, allLikes bool) ([]feedItem, bool, error) {
	posts := d.cachedPosts()
	ownID, _, _ := d.store.Identity()
	start := page * feedPageLimit
	end := start + feedPageLimit

	// cachedPosts is already newest-first, so a page is a slice of the
	// filtered view. Filtering mine once per call is O(n); the unfiltered
	// path is O(1) slicing. Re-scanning from index 0 each page would be
	// O(n^2) over a full fetch and hang the CLI on a large feed.
	var view []core.SignedEvent
	if mine {
		view = make([]core.SignedEvent, 0, len(posts))
		for _, p := range posts {
			if p.Author == ownID {
				view = append(view, p)
			}
		}
	} else {
		view = posts
	}

	if start >= len(view) {
		return nil, false, nil
	}
	if end > len(view) {
		end = len(view)
	}
	items := make([]feedItem, 0, end-start)
	for _, p := range view[start:end] {
		items = append(items, d.feedItemFor(p))
	}
	if allLikes {
		ids := make([]core.EventID, 0, len(items))
		for _, it := range items {
			if it.Author == ownID.String() {
				if id, err := core.ParseEventID(it.ID); err == nil {
					ids = append(ids, id)
				}
			}
		}
		if len(ids) > 0 {
			_, _ = d.handleFetchLikes(ids)
		}
	}
	d.populateLikes(items)
	return items, end < len(view), nil
}

// feedItemFor builds a feedItem from one signed post event. A post with a
// non-zero ParentID is a reply; the feed prefixes it so replies are visually
// distinguishable from top-level posts.
func (d *Daemon) feedItemFor(p core.SignedEvent) feedItem {
	text := p.Event.Post.Text
	if p.Event.Post != nil && !p.Event.Post.ParentID.IsZero() {
		text = "(reply) " + text
	}
	name := d.displayName(p.Author)
	id, _ := p.ID()
	return feedItem{
		ID:        id.String(),
		Timestamp: p.Event.Timestamp,
		Author:    p.Author.String(),
		Name:      name,
		Text:      text,
	}
}

// feedItemsFor builds feedItems for a batch of merged post/reply events,
// used to push a feed diff after a sync round.
func (d *Daemon) feedItemsFor(posts []core.SignedEvent) []feedItem {
	items := make([]feedItem, 0, len(posts))
	for _, p := range posts {
		items = append(items, d.feedItemFor(p))
	}
	return items
}
