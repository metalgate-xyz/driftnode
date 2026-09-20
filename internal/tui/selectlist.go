package tui

import (
	"strings"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
)

// rowRenderer renders one item of a selectable list into a string (which may
// span multiple lines). The selected flag lets the renderer tint the active
// row, matching the styling the old bubbles/list delegates applied.
type rowRenderer[T any] func(item T, selected bool) string

// selectList is a scrollable, selectable list built on viewport.Model. Unlike
// bubbles/list it has no title bar, no filter bar, and no paginator: the whole
// box scrolls one line at a time and the selected item simply moves up and
// down, so every tab shares the feed's continuous-scroll feel. The viewport
// keeps the scroll position pinned when the content changes underneath.
type selectList[T any] struct {
	vp        viewport.Model
	renderRow rowRenderer[T]
	items     []T
	selected  int
}

// newSelectList builds a selectList sized to the panel's content box.
func newSelectList[T any](w, h int, render rowRenderer[T]) selectList[T] {
	v := viewport.New(viewport.WithWidth(w), viewport.WithHeight(h))
	v.MouseWheelEnabled = true
	v.SoftWrap = false
	return selectList[T]{vp: v, renderRow: render}
}

// resize sets the inner content size of the list.
func (s *selectList[T]) resize(w, h int) {
	s.vp.SetWidth(w)
	s.vp.SetHeight(h)
	s.resync()
}

// setItems replaces the list contents, clamps the selection to the new range,
// and re-renders the content while keeping the scroll position stable.
func (s *selectList[T]) setItems(items []T) {
	s.items = items
	if s.selected >= len(s.items) {
		s.selected = len(s.items) - 1
	}
	if s.selected < 0 {
		s.selected = 0
	}
	s.resync()
}

// resync rebuilds the rendered content and keeps the selected row in view.
func (s *selectList[T]) resync() {
	if len(s.items) == 0 {
		s.vp.SetContent("")
		return
	}
	var b strings.Builder
	for i, it := range s.items {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(s.renderRow(it, i == s.selected))
	}
	s.vp.SetContent(b.String())
	s.ensureVisible()
}

// ensureVisible scrolls just enough that the selected item is fully inside the
// viewport window. A multi-line item is treated as a block: its top row is
// selected*itemHeight and it spans itemHeight rows.
func (s *selectList[T]) ensureVisible() {
	if len(s.items) == 0 || s.vp.Height() == 0 {
		return
	}
	h := s.itemHeight()
	if h == 0 {
		return
	}
	top := s.selected * h
	bottom := top + h
	y := s.vp.YOffset()
	visible := s.vp.Height()
	if bottom > y+visible {
		s.vp.SetYOffset(bottom - visible)
	} else if top < y {
		s.vp.SetYOffset(top)
	}
}

// itemHeight is the number of rows one item occupies. The renderer emits the
// same number of lines for every item (the rows are single-line title and
// description), so we measure the first item and fall back to 1 when empty.
func (s *selectList[T]) itemHeight() int {
	if len(s.items) == 0 {
		return 1
	}
	return strings.Count(s.renderRow(s.items[0], false), "\n") + 1
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
// of the viewport's content box (0 is the first visible row), so the caller
// must first subtract the screen rows above the panel. Out-of-range rows are
// ignored.
func (s *selectList[T]) click(row int) {
	if len(s.items) == 0 || row < 0 || s.vp.Height() == 0 {
		return
	}
	h := s.itemHeight()
	if h == 0 {
		return
	}
	idx := (row + s.vp.YOffset()) / h
	if idx < 0 || idx >= len(s.items) {
		return
	}
	s.selected = idx
	s.resync()
}

// update applies a message. Navigation keys move the selection and the
// viewport follows; mouse wheel and page keys scroll the viewport directly.
func (s selectList[T]) update(msg tea.Msg) (selectList[T], tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		switch msg.String() {
		case "up", "k":
			s.move(-1)
		case "down", "j":
			s.move(1)
		case "pgup":
			s.vp.PageUp()
		case "pgdown":
			s.vp.PageDown()
		case "home", "g":
			if len(s.items) == 0 {
				return s, nil
			}
			s.selected = 0
			s.resync()
		case "end", "G":
			if len(s.items) == 0 {
				return s, nil
			}
			s.selected = len(s.items) - 1
			s.resync()
		default:
			v, cmd := s.vp.Update(msg)
			s.vp = v
			return s, cmd
		}
		return s, nil
	case tea.MouseWheelMsg:
		v, cmd := s.vp.Update(msg)
		s.vp = v
		return s, cmd
	}
	return s, nil
}

// move shifts the selection by delta and scrolls just enough to keep the
// selected item in view.
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
	s.resync()
}

// view returns the viewport's current view.
func (s selectList[T]) view() string {
	return s.vp.View()
}
