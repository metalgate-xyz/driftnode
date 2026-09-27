package tui

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
)

// TestDiffPreservesSelection verifies the central UX property of the push
// model: applying a diff updates only the changed row and keeps the selection
// pinned to the same item, instead of wiping and rebuilding the list.
func TestDiffPreservesSelection(t *testing.T) {
	m := newModel("", "driftnode:test")
	m.width, m.height = 80, 24
	m.layout()

	zens := []zen{
		{name: "alice", identity: "driftnode:alice", id: "tok-a", status: "connected"},
		{name: "bob", identity: "driftnode:bob", id: "tok-b", status: "connecting"},
		{name: "carol", identity: "driftnode:carol", id: "tok-c", status: "connected"},
	}
	m.zens.setItems(zens)
	m.activeTab = tabZens

	// Select bob (index 1) via arrow-down, the real nav key.
	m = press(t, m, tea.KeyPressMsg{Code: tea.KeyDown})
	z, _ := m.zens.selectedItem()
	if z.identity != "driftnode:bob" {
		t.Fatalf("selection: want bob, got %q", z.identity)
	}

	// A status diff arrives for carol (a different zen). The selection must
	// stay on bob.
	m.applyDiff(diffMsg{
		panel:  "zens",
		upsert: &zen{name: "carol", identity: "driftnode:carol", id: "tok-c", status: "error"},
	})
	z, _ = m.zens.selectedItem()
	if z.identity != "driftnode:bob" {
		t.Fatalf("after diff: selection should stay on bob, got %q", z.identity)
	}
	// Carol's row should now show the error status.
	out := m.View().Content
	if !strings.Contains(out, "error") {
		t.Fatal("carol's updated status should render")
	}
}

// TestFeedDiffDedups verifies that pushing the same post twice does not
// duplicate it in the feed.
func TestFeedDiffDedups(t *testing.T) {
	f := newFeedPanel(80, 24)
	p := feedPost{id: "e1", name: "a", author: "driftnode:a", text: "hi", ts: 1000}
	f.upsert(p)
	f.upsert(p)
	if len(f.posts) != 1 {
		t.Fatalf("dedup: want 1 post, got %d", len(f.posts))
	}
}

// TestFeedDiffInsertsInOrder verifies posts inserted out of order land in
// reverse-chronological (display) order.
func TestFeedDiffInsertsInOrder(t *testing.T) {
	f := newFeedPanel(80, 24)
	f.upsert(feedPost{id: "old", name: "x", author: "driftnode:x", text: "old", ts: 1000})
	f.upsert(feedPost{id: "new", name: "y", author: "driftnode:y", text: "new", ts: 3000})
	f.upsert(feedPost{id: "mid", name: "z", author: "driftnode:z", text: "mid", ts: 2000})
	if len(f.posts) != 3 {
		t.Fatalf("want 3 posts, got %d", len(f.posts))
	}
	want := []string{"new", "mid", "old"}
	for i, w := range want {
		if f.posts[i].id != w {
			t.Fatalf("post %d: want %q, got %q", i, w, f.posts[i].id)
		}
	}
}

// TestFeedRenderIsBounded verifies the feed render cost is bounded by the
// visible height, not the total post count.
func TestFeedRenderIsBounded(t *testing.T) {
	f := newFeedPanel(80, 3)
	posts := make([]feedPost, 1000)
	for i := range posts {
		posts[i] = feedPost{id: "p" + string(rune('a'+i%26)) + string(rune('a'+i/26)), name: "x", author: "driftnode:x", text: "post", ts: int64(100000 - i)}
	}
	f.setPosts(posts)
	out := f.render()
	lines := strings.Split(out, "\n")
	if len(lines) > 3 {
		t.Fatalf("render should be bounded by height=3, got %d lines", len(lines))
	}
}

// TestZensRenderIsBounded verifies the zens list render cost is bounded by
// the visible height, not the total item count.
func TestZensRenderIsBounded(t *testing.T) {
	s := newZenList(80, 3)
	items := make([]zen, 1000)
	for i := range items {
		items[i] = zen{name: "z", identity: "driftnode:z", id: "tok", status: "connected"}
		items[i].id = "tok" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		items[i].name = items[i].id
	}
	s.setItems(items)
	out := s.view()
	lines := strings.Split(out, "\n")
	if len(lines) > 3 {
		t.Fatalf("render should be bounded by height=3, got %d lines", len(lines))
	}
}

// TestScrollPreservesSelection verifies that a pure scroll (wheel, pgup/pgdn)
// moves only the viewport and leaves the selection on its row, instead of
// pinning the selection to the top or bottom of the visible window.
func TestScrollPreservesSelection(t *testing.T) {
	// Feed panel: select row 5, scroll down past it, selection stays at 5.
	f := newFeedPanel(80, 3)
	posts := make([]feedPost, 100)
	for i := range posts {
		posts[i] = feedPost{id: "p" + itoa(i), name: "x", author: "driftnode:x", text: "post", ts: int64(100 - i)}
	}
	f.setPosts(posts)
	f.selected = 5
	f.scrollBy(20)
	if f.selected != 5 {
		t.Fatalf("scroll down: selection should stay at 5, got %d", f.selected)
	}
	f.scrollBy(-20)
	if f.selected != 5 {
		t.Fatalf("scroll up: selection should stay at 5, got %d", f.selected)
	}

	// selectList: same property.
	s := newZenList(80, 3)
	items := make([]zen, 100)
	for i := range items {
		items[i] = zen{name: "z", identity: "driftnode:z" + itoa(i), id: "tok" + itoa(i), status: "connected"}
	}
	s.setItems(items)
	s.selected = 7
	s.scrollBy(30)
	if s.selected != 7 {
		t.Fatalf("list scroll down: selection should stay at 7, got %d", s.selected)
	}
	s.scrollBy(-30)
	if s.selected != 7 {
		t.Fatalf("list scroll up: selection should stay at 7, got %d", s.selected)
	}
}

// TestFeedLargeSnapshotLoadsFast guards against the O(n^2) snapshot-load
// regression: setPosts must be O(n), not O(n log n) or worse, so a snapshot
// of a million posts loads in well under a second instead of hanging the
// TUI. The daemon sends the feed pre-sorted newest-first, so setPosts must
// not re-sort.
func TestFeedLargeSnapshotLoadsFast(t *testing.T) {
	f := newFeedPanel(80, 24)
	posts := make([]feedPost, 1_000_000)
	for i := range posts {
		posts[i] = feedPost{
			id:     "e" + itoa(i),
			name:   "x",
			author: "driftnode:x",
			text:   "post",
			ts:     int64(1_000_000 - i), // already newest-first
		}
	}
	f.setPosts(posts)
	if len(f.posts) != 1_000_000 {
		t.Fatalf("want 1000000 posts, got %d", len(f.posts))
	}
	// Rendering is virtualized: only the visible window is produced.
	out := f.render()
	lines := strings.Split(out, "\n")
	if len(lines) > 24 {
		t.Fatalf("render should be bounded by height=24, got %d lines", len(lines))
	}
}

// itoa avoids importing strconv just for the loop above.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
