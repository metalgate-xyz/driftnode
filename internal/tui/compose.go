package tui

import (
	"strings"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// compose is the post input line. It wraps bubbles/textinput so we get a real
// cursor, line editing and width handling for free instead of hand-rolling
// a buffer.
type compose struct {
	input textinput.Model
}

func newCompose() compose {
	ti := textinput.New()
	ti.Prompt = "❯ "
	ti.Placeholder = "write a post and press enter…"
	ti.CharLimit = 280
	s := textinput.DefaultStyles(true)
	s.Focused.Prompt = lipgloss.NewStyle().Foreground(lipgloss.Color(accent)).Bold(true)
	s.Blurred.Prompt = lipgloss.NewStyle().Foreground(lipgloss.Color(muted))
	ti.SetStyles(s)
	return compose{input: ti}
}

// focus captures the textinput's focus command and tints the prompt.
func (c *compose) focus() tea.Cmd {
	return c.input.Focus()
}

func (c *compose) blur() {
	c.input.Blur()
}

func (c *compose) reset() { c.input.Reset() }

func (c compose) value() string { return strings.TrimSpace(c.input.Value()) }

func (c compose) update(msg tea.Msg) (compose, tea.Cmd) {
	m, cmd := c.input.Update(msg)
	c.input = m
	return c, cmd
}

// render returns the input styled to span the full width. The active/focused
// prompt is accent-colored; the blurred prompt stays muted. MaxHeight(1)
// clips the single-line input to its row budget so it can never overflow the
// fixed layout.
func (c compose) render(w int) string {
	return theme.statusBar.Width(w).MaxHeight(1).Render(c.input.View())
}
