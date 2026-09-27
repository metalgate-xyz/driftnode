package tui

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// feedPanel renders the merged timeline as a virtualized, scrollable list.
// Only the visible window of rows is rendered on each paint, so a repaint is
// O(visible height) and independent of the total number of posts.
//
// Posts are kept newest-first (reverse-chronological). upsert inserts a post
// by event ID at the position its timestamp dictates, so the feed updates in
// place without rebuilding from scratch: scroll position and selection, keyed
// by post ID, survive.
type feedPanel struct {
	width    int
	height   int
	yOffset  int
	posts    []feedPost
	selected int
}

func newFeedPanel(w, h int) feedPanel {
	return feedPanel{width: w, height: h}
}

// panelWidth returns the panel's inner width. Used by layout to detect first sizing.
func (f *feedPanel) panelWidth() int { return f.width }

// resize sets the inner content size of the panel.
func (f *feedPanel) resize(w, h int) {
	f.width, f.height = w, h
	f.clampYOffset()
}

// setPosts replaces the whole feed. The daemon's snapshot is already
// reverse-chronological (newest-first), so we keep it as-is rather than
// re-sorting. Re-sorting here would be O(n log n) and, on a large feed,
// blocks the TUI for seconds on every snapshot.
func (f *feedPanel) setPosts(posts []feedPost) {
	f.posts = posts
	f.clampSelection()
	f.clampYOffset()
}

// postLess reports whether a should sort before b (a is newer): larger
// timestamp first, then larger id as a stable tiebreak.
func (f feedPanel) postLess(a, b feedPost) bool {
	if a.ts != b.ts {
		return a.ts > b.ts
	}
	return a.id > b.id
}

// upsert inserts or replaces a post by id at the position its timestamp
// dictates. Duplicate pushes are no-ops. This is the incremental path: a
// single new post costs O(n) to find its slot, but only the changed row
// moves and the selection stays pinned to its post id.
func (f *feedPanel) upsert(p feedPost) {
	if p.id == "" {
		return
	}
	for i, ex := range f.posts {
		if ex.id == p.id {
			f.posts[i] = p
			return
		}
	}
	// Insert keeping newest-first order.
	pos := len(f.posts)
	for i, ex := range f.posts {
		if f.postLess(p, ex) {
			pos = i
			break
		}
	}
	f.posts = append(f.posts, feedPost{})
	copy(f.posts[pos+1:], f.posts[pos:])
	f.posts[pos] = p
	// Keep the selection on the same post: if the new post slotted above the
	// selection, shift the selection down so the highlighted row doesn't
	// jump to the new post.
	if pos <= f.selected {
		f.selected++
	}
	f.clampSelection()
	f.clampYOffset()
}

// remove drops a post by id. If it was above the selection, the selection
// shifts up so it stays on the same logical post.
func (f *feedPanel) remove(id string) {
	for i, ex := range f.posts {
		if ex.id == id {
			f.posts = append(f.posts[:i], f.posts[i+1:]...)
			if i < f.selected {
				f.selected--
			}
			f.clampSelection()
			f.clampYOffset()
			return
		}
	}
}

// render builds only the visible window of rows. The cost is bounded by the
// viewport height, not the number of posts.
func (f feedPanel) render() string {
	if f.height <= 0 {
		return ""
	}
	if len(f.posts) == 0 {
		return theme.hint.Render("(no posts yet)")
	}
	var b strings.Builder
	for row := 0; row < f.height; row++ {
		idx := f.yOffset + row
		if idx >= len(f.posts) {
			break
		}
		if row > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(f.renderRow(f.posts[idx], idx == f.selected))
	}
	return b.String()
}

// renderRow styles one post line. The selected row carries the highlight
// background, mirroring the selectable lists.
func (f feedPanel) renderRow(p feedPost, selected bool) string {
	display := p.name
	if display == "" {
		display = p.author
	}

	authorStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(accent))
	ageStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(muted))
	if selected {
		authorStyle = authorStyle.Background(lipgloss.Color("#282a40"))
		ageStyle = ageStyle.Background(lipgloss.Color("#282a40")).Foreground(lipgloss.Color(accent))
	}

	author := authorStyle.Render(display)
	age := ageStyle.Render(p.age)
	return fmt.Sprintf("%s  %s  %s", author, p.text, age)
}

func (f feedPanel) update(msg tea.Msg) (feedPanel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		switch msg.String() {
		case "up", "k":
			f.move(-1)
			return f, nil
		case "down", "j":
			f.move(1)
			return f, nil
		case "pgup":
			f.scrollBy(-f.maxScrollStep())
			return f, nil
		case "pgdown":
			f.scrollBy(f.maxScrollStep())
			return f, nil
		case "home", "g":
			if len(f.posts) == 0 {
				return f, nil
			}
			f.selected = 0
			f.clampYOffset()
			return f, nil
		case "end", "G":
			if len(f.posts) == 0 {
				return f, nil
			}
			f.selected = len(f.posts) - 1
			f.ensureVisible()
			return f, nil
		}
	case tea.MouseWheelMsg:
		switch msg.Button {
		case tea.MouseWheelUp:
			f.scrollBy(-f.vpWheelStep())
		case tea.MouseWheelDown:
			f.scrollBy(f.vpWheelStep())
		}
		return f, nil
	}
	return f, nil
}

// vpWheelStep mirrors the bubbles/viewport default mouse-wheel delta.
func (f feedPanel) vpWheelStep() int { return 3 }

// maxScrollStep is the page size for pgup/pgdn.
func (f feedPanel) maxScrollStep() int {
	if f.height-1 > 0 {
		return f.height - 1
	}
	return 1
}

// move shifts the selection by delta and keeps the selected row in view.
func (f *feedPanel) move(delta int) {
	if len(f.posts) == 0 {
		return
	}
	f.selected += delta
	if f.selected < 0 {
		f.selected = 0
	}
	if f.selected >= len(f.posts) {
		f.selected = len(f.posts) - 1
	}
	f.ensureVisible()
}

// scrollBy shifts the viewport by delta lines, leaving the selection
// untouched. A pure scroll (wheel, pgup/pgdn) moves only the viewport; the
// selection stays on whatever row it was on.
func (f *feedPanel) scrollBy(delta int) {
	f.yOffset += delta
	f.clampYOffset()
}

// ensureVisible scrolls just enough that the selected row stays in view.
func (f *feedPanel) ensureVisible() {
	if len(f.posts) == 0 || f.height == 0 {
		return
	}
	top := f.selected
	bottom := top + 1
	if bottom > f.yOffset+f.height {
		f.yOffset = bottom - f.height
	} else if top < f.yOffset {
		f.yOffset = top
	}
	f.clampYOffset()
}

// clampYOffset keeps the scroll position within the valid range.
func (f *feedPanel) clampYOffset() {
	max := f.maxYOffset()
	if f.yOffset < 0 {
		f.yOffset = 0
	}
	if f.yOffset > max {
		f.yOffset = max
	}
}

// maxYOffset is the largest valid top row.
func (f feedPanel) maxYOffset() int {
	if len(f.posts) <= f.height {
		return 0
	}
	return len(f.posts) - f.height
}

// clampSelection keeps the selection inside the post range.
func (f *feedPanel) clampSelection() {
	if len(f.posts) == 0 {
		f.selected = 0
		return
	}
	if f.selected < 0 {
		f.selected = 0
	}
	if f.selected >= len(f.posts) {
		f.selected = len(f.posts) - 1
	}
}

// click selects the post under the given row. The row is relative to the top
// of the panel's content box (0 is the first visible row). Out-of-range rows
// are ignored.
func (f *feedPanel) click(row int) {
	if len(f.posts) == 0 || row < 0 || f.height == 0 {
		return
	}
	idx := row + f.yOffset
	if idx < 0 || idx >= len(f.posts) {
		return
	}
	f.selected = idx
}

// selectedItem returns the post under the selection cursor, or ok=false when
// the feed is empty.
func (f feedPanel) selectedItem() (feedPost, bool) {
	if f.selected < 0 || f.selected >= len(f.posts) {
		return feedPost{}, false
	}
	return f.posts[f.selected], true
}
