package core

import (
	"sort"
	"time"
)

// Log holds the signed events of a single identity's log, in append order. It
// is the in-memory form used for projection and feed construction. The store
// persists events individually; this type is for computing derived state.
type Log struct {
	Events []SignedEvent
}

// NewLog constructs a Log from a set of signed events.
func NewLog(events []SignedEvent) *Log {
	return &Log{Events: events}
}

// SortedByTime returns the events ordered by timestamp then sequence, so two
// zens with different partial views compute the same order.
func (l *Log) SortedByTime() []SignedEvent {
	out := make([]SignedEvent, len(l.Events))
	copy(out, l.Events)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Event.Timestamp != out[j].Event.Timestamp {
			return out[i].Event.Timestamp < out[j].Event.Timestamp
		}
		return out[i].Event.Sequence < out[j].Event.Sequence
	})
	return out
}

// Profile is the current profile for an identity, computed as the
// last-write-wins projection over Profile-log events (§7.1).
func (l *Log) Profile() *Profile {
	var latest *Profile
	var latestTS int64
	for _, se := range l.Events {
		if se.Event.Kind != KindProfile || se.Event.Profile == nil {
			continue
		}
		if se.Event.Timestamp >= latestTS {
			latest = se.Event.Profile
			latestTS = se.Event.Timestamp
		}
	}
	return latest
}

// Detail is the current personal metadata for an identity, computed as the
// last-write-wins projection over Detail-log events. Unlike Profile, the
// DetailLog is never crawled or durably cached by peers, so this projection
// is run against events fetched on demand for display.
func (l *Log) Detail() *Detail {
	var latest *Detail
	var latestTS int64
	for _, se := range l.Events {
		if se.Event.Kind != KindDetail || se.Event.Detail == nil {
			continue
		}
		if se.Event.Timestamp >= latestTS {
			latest = se.Event.Detail
			latestTS = se.Event.Timestamp
		}
	}
	return latest
}

// FollowSet is the set of pubkeys currently followed, computed by replaying
// Follow and Unfollow events in timestamp order (§7.1).
type FollowSet struct {
	set map[[32]byte]bool
}

// NewFollowSet constructs an empty follow set.
func NewFollowSet() *FollowSet { return &FollowSet{set: make(map[[32]byte]bool)} }

// Contains reports whether target is currently followed.
func (f *FollowSet) Contains(target [32]byte) bool { return f.set[target] }

// Each calls fn for each currently-followed pubkey.
func (f *FollowSet) Each(fn func(target [32]byte)) {
	for k := range f.set {
		fn(k)
	}
}

// Len returns the number of currently-followed identities.
func (f *FollowSet) Len() int { return len(f.set) }

// Apply replays a Follow or Unfollow event, mutating the set. Other kinds are
// ignored.
func (f *FollowSet) Apply(se SignedEvent) {
	switch se.Event.Kind {
	case KindFollow:
		if se.Event.Follow != nil {
			f.set[se.Event.Follow.TargetPubkey] = true
		}
	case KindUnfollow:
		if se.Event.Follow != nil {
			delete(f.set, se.Event.Follow.TargetPubkey)
		}
	}
}

// FollowSet projects the full Profile log into the current follow set by
// replaying events in timestamp order (§7.1).
func (l *Log) FollowSet() *FollowSet {
	fs := NewFollowSet()
	for _, se := range l.SortedByTime() {
		fs.Apply(se)
	}
	return fs
}

// Tombstoned returns the set of event IDs deleted by Delete events in this
// log. A deleted event is a tombstone, not erasure (§7.2): zens who already
// replicated the original may still hold a copy.
func (l *Log) Tombstoned() map[EventID]bool {
	out := make(map[EventID]bool)
	for _, se := range l.Events {
		if se.Event.Kind != KindDelete || se.Event.Delete == nil {
			continue
		}
		out[se.Event.Delete.TargetID] = true
	}
	return out
}

// NonTombstonedPosts returns Post and Reply events whose own ID is not
// tombstoned within this log. Delete is a tombstone referencing a target ID;
// the deleted event itself may live in the same log.
func (l *Log) NonTombstonedPosts() []SignedEvent {
	tomb := l.Tombstoned()
	var out []SignedEvent
	for _, se := range l.SortedByTime() {
		if se.Event.Kind != KindPost && se.Event.Kind != KindReply {
			continue
		}
		id, err := se.ID()
		if err != nil {
			continue
		}
		if tomb[id] {
			continue
		}
		out = append(out, se)
	}
	return out
}

// Posts returns the non-tombstoned Post and Reply events, reverse-chronological.
func (l *Log) Posts() []SignedEvent {
	posts := l.NonTombstonedPosts()
	sort.SliceStable(posts, func(i, j int) bool {
		return posts[i].Event.Timestamp > posts[j].Event.Timestamp
	})
	return posts
}

// FormatTime renders a unix-nanosecond timestamp as RFC3339 for display.
func FormatTime(ts int64) string {
	return time.Unix(0, ts).UTC().Format(time.RFC3339)
}
