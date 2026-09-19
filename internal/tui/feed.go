package tui

import (
	"fmt"
	"strings"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// feedPanel renders the merged timeline in a scrollable viewport.
type feedPanel struct {
	vp viewport.Model
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
}

// setPosts rebuilds the feed content, keeping the scroll position stable.
func (f *feedPanel) setPosts(posts []feedPost, s styles, w int) {
	if len(posts) == 0 {
		f.vp.SetContent(s.hint.Render("(no posts yet)"))
		return
	}
	// Size the author column to the widest display name so rows never wrap.
	authorCol := len("author")
	for _, p := range posts {
		display := p.name
		if display == "" {
			display = p.author
		}
		if len(display) > authorCol {
			authorCol = len(display)
		}
	}
	contentW := w - theme.panel.GetHorizontalFrameSize()
	textW := contentW - authorCol - 8
	if textW < 8 {
		textW = 8
	}
	var b strings.Builder
	for _, p := range posts {
		display := p.name
		if display == "" {
			display = p.author
		}
		author := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(accent)).Render(padRight(display, authorCol))
		text := truncate(p.text, textW)
		age := lipgloss.NewStyle().Foreground(lipgloss.Color(muted)).Render(p.age)
		fmt.Fprintf(&b, "%s  %s  %s\n", author, text, age)
	}
	f.vp.SetContent(strings.TrimRight(b.String(), "\n"))
}

func (f feedPanel) update(msg tea.Msg) (feedPanel, tea.Cmd) {
	v, cmd := f.vp.Update(msg)
	f.vp = v
	return f, cmd
}

func (f feedPanel) render() string {
	return f.vp.View()
}
