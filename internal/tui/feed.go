package tui

import (
	"fmt"
	"strings"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// feedPanel renders the merged timeline in a scrollable viewport. It tracks a
// selected post so the active row can be highlighted like the other tabs.
type feedPanel struct {
	vp       viewport.Model
	posts    []feedPost
	selected int
}

func newFeedPanel(w, h int) feedPanel {
	v := viewport.New(viewport.WithWidth(w), viewport.WithHeight(h))
	v.MouseWheelEnabled = true
	v.SoftWrap = false
	return feedPanel{vp: v}
}

func (f *feedPanel) resize(w, h int) {
	f.vp.SetWidth(w)
	f.vp.SetHeight(h)
	f.renderPosts()
}

// setPosts rebuilds the feed content, keeping the scroll position stable.
func (f *feedPanel) setPosts(posts []feedPost) {
	f.posts = posts
	if f.selected >= len(f.posts) {
		f.selected = len(f.posts) - 1
	}
	if f.selected < 0 {
		f.selected = 0
	}
	f.renderPosts()
}

// renderPosts rebuilds the viewport content, highlighting the selected row.
func (f *feedPanel) renderPosts() {
	if len(f.posts) == 0 {
		f.vp.SetContent(theme.hint.Render("(no posts yet)"))
		return
	}
	var b strings.Builder
	for i, p := range f.posts {
		display := p.name
		if display == "" {
			display = p.author
		}
		selected := i == f.selected

		authorStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(accent))
		ageStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(muted))
		if selected {
			authorStyle = authorStyle.Background(lipgloss.Color("#282a40"))
			ageStyle = ageStyle.Background(lipgloss.Color("#282a40")).Foreground(lipgloss.Color(accent))
		}

		author := authorStyle.Render(display)
		age := ageStyle.Render(p.age)
		fmt.Fprintf(&b, "%s  %s  %s\n", author, p.text, age)
	}
	f.vp.SetContent(strings.TrimRight(b.String(), "\n"))
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
		case "home", "g":
			if len(f.posts) == 0 {
				return f, nil
			}
			f.selected = 0
			f.renderPosts()
			f.ensureVisible()
			return f, nil
		case "end", "G":
			if len(f.posts) == 0 {
				return f, nil
			}
			f.selected = len(f.posts) - 1
			f.renderPosts()
			f.ensureVisible()
			return f, nil
		}
	}
	// Fall back to viewport scrolling (pgup/pgdn, mouse wheel, etc.).
	v, cmd := f.vp.Update(msg)
	f.vp = v
	return f, cmd
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
	f.renderPosts()
	f.ensureVisible()
}

// ensureVisible scrolls just enough that the selected row stays in view.
func (f *feedPanel) ensureVisible() {
	if len(f.posts) == 0 || f.vp.Height() == 0 {
		return
	}
	top := f.selected
	bottom := top + 1
	y := f.vp.YOffset()
	visible := f.vp.Height()
	if bottom > y+visible {
		f.vp.SetYOffset(bottom - visible)
	} else if top < y {
		f.vp.SetYOffset(top)
	}
}

// click selects the post under the given row. The row is relative to the top
// of the viewport's content box (0 is the first visible row). Out-of-range
// rows are ignored.
func (f *feedPanel) click(row int) {
	if len(f.posts) == 0 || row < 0 || f.vp.Height() == 0 {
		return
	}
	idx := row + f.vp.YOffset()
	if idx < 0 || idx >= len(f.posts) {
		return
	}
	f.selected = idx
	f.renderPosts()
}

func (f feedPanel) render() string {
	return f.vp.View()
}
