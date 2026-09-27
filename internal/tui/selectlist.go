package tui

import (
	"strings"

	tea "charm.land/bubbletea/v2"
)

// rowRenderer renders one item of a selectable list into a string (which may
// span multiple lines). The selected flag lets the renderer tint the active
// row, matching the styling the old bubbles/list delegates applied.
type rowRenderer[T any] func(item T, selected bool) string

// itemKey returns the stable identity key for an item, used to upsert and
// remove by identity so a diff moves only the changed row instead of
// rebuilding the whole list.
type itemKey[T any] func(item T) string

// selectList is a virtualized, scrollable, selectable list. Only the visible
// window of rows is rendered on each paint, so a repaint is O(visible
// height) and independent of the total item count. The list applies
// upserts/removes by key, so the scroll position and selection survive
// incremental updates instead of being wiped on every refresh.
type selectList[T any] struct {
	width     int
	height    int
	yOffset   int
	renderRow rowRenderer[T]
	key       itemKey[T]
	items     []T
	selected  int
}

// newSelectList builds a selectList sized to the panel's content box.
func newSelectList[T any](w, h int, render rowRenderer[T], key itemKey[T]) selectList[T] {
	return selectList[T]{width: w, height: h, renderRow: render, key: key}
}

// resize sets the inner content size of the list and clamps the scroll.
func (s *selectList[T]) resize(w, h int) {
	s.width, s.height = w, h
	s.clampYOffset()
}

// setItems replaces the list contents, clamps the selection to the new range,
// and re-clamps the scroll. Used for the initial snapshot; later updates go
// through upsert/remove so only changed rows move.
func (s *selectList[T]) setItems(items []T) {
	s.items = items
	s.clampSelection()
	s.clampYOffset()
}

// upsert adds or replaces an item by key. Existing items keep their
// position; a new item is appended. The selection is preserved when possible.
func (s *selectList[T]) upsert(item T) {
	id := s.key(item)
	for i, it := range s.items {
		if s.key(it) == id {
			s.items[i] = item
			s.clampSelection()
			return
		}
	}
	s.items = append(s.items, item)
	s.clampSelection()
}

// remove drops the item with the given key, keeping the selection stable.
func (s *selectList[T]) remove(id string) {
	for i, it := range s.items {
		if s.key(it) == id {
			s.items = append(s.items[:i], s.items[i+1:]...)
			break
		}
	}
	s.clampSelection()
	s.clampYOffset()
}

// selectedItem returns the item under the selection cursor, or ok=false when
// the list is empty.
func (s selectList[T]) selectedItem() (T, bool) {
	var zero T
	if s.selected < 0 || s.selected >= len(s.items) {
		return zero, false
	}
	return s.items[s.selected], true
}

// allItems returns the current item slice (read-only).
func (s selectList[T]) allItems() []T { return s.items }

// click selects the item under the given row. The row is relative to the top
// of the panel's content box (0 is the first visible row). Out-of-range rows
// are ignored.
func (s *selectList[T]) click(row int) {
	if len(s.items) == 0 || row < 0 || s.height == 0 {
		return
	}
	idx := row + s.yOffset
	if idx < 0 || idx >= len(s.items) {
		return
	}
	s.selected = idx
}

// update applies a message. Navigation keys move the selection and keep it
// in view; mouse wheel and page keys scroll.
func (s selectList[T]) update(msg tea.Msg) (selectList[T], tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		switch msg.String() {
		case "up", "k":
			s.move(-1)
		case "down", "j":
			s.move(1)
		case "pgup":
			s.scrollBy(-s.pageStep())
		case "pgdown":
			s.scrollBy(s.pageStep())
		case "home", "g":
			if len(s.items) == 0 {
				return s, nil
			}
			s.selected = 0
			s.clampYOffset()
		case "end", "G":
			if len(s.items) == 0 {
				return s, nil
			}
			s.selected = len(s.items) - 1
			s.ensureVisible()
		}
		return s, nil
	case tea.MouseWheelMsg:
		switch msg.Button {
		case tea.MouseWheelUp:
			s.scrollBy(-3)
		case tea.MouseWheelDown:
			s.scrollBy(3)
		}
		return s, nil
	}
	return s, nil
}

// move shifts the selection by delta and scrolls to keep it visible.
func (s *selectList[T]) move(delta int) {
	if len(s.items) == 0 {
		return
	}
	s.selected += delta
	if s.selected < 0 {
		s.selected = 0
	}
	if s.selected >= len(s.items) {
		s.selected = len(s.items) - 1
	}
	s.ensureVisible()
}

// scrollBy shifts the viewport by delta lines, leaving the selection
// untouched. A pure scroll (wheel, pgup/pgdn) moves only the viewport; the
// selection stays on whatever row it was on.
func (s *selectList[T]) scrollBy(delta int) {
	s.yOffset += delta
	s.clampYOffset()
}

// pageStep is the page size for pgup/pgdn.
func (s selectList[T]) pageStep() int {
	if s.height-1 > 0 {
		return s.height - 1
	}
	return 1
}

// ensureVisible scrolls just enough that the selected row stays in view.
func (s *selectList[T]) ensureVisible() {
	if len(s.items) == 0 || s.height == 0 {
		return
	}
	if s.selected+1 > s.yOffset+s.height {
		s.yOffset = s.selected + 1 - s.height
	} else if s.selected < s.yOffset {
		s.yOffset = s.selected
	}
	s.clampYOffset()
}

// clampYOffset keeps the scroll position within the valid range.
func (s *selectList[T]) clampYOffset() {
	max := s.maxYOffset()
	if s.yOffset < 0 {
		s.yOffset = 0
	}
	if s.yOffset > max {
		s.yOffset = max
	}
}

func (s selectList[T]) maxYOffset() int {
	if len(s.items) <= s.height {
		return 0
	}
	return len(s.items) - s.height
}

// clampSelection keeps the selection inside the item range.
func (s *selectList[T]) clampSelection() {
	if len(s.items) == 0 {
		s.selected = 0
		return
	}
	if s.selected < 0 {
		s.selected = 0
	}
	if s.selected >= len(s.items) {
		s.selected = len(s.items) - 1
	}
}

// view renders only the visible window of rows. The cost is bounded by the
// viewport height, not by the number of items.
func (s selectList[T]) view() string {
	if s.height <= 0 {
		return ""
	}
	if len(s.items) == 0 {
		return ""
	}
	var b strings.Builder
	for row := 0; row < s.height; row++ {
		idx := s.yOffset + row
		if idx >= len(s.items) {
			break
		}
		if row > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(s.renderRow(s.items[idx], idx == s.selected))
	}
	return b.String()
}
