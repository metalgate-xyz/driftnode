package tui

import (
	"fmt"

	tea "charm.land/bubbletea/v2"
)

// handleKey routes a key press. Global keys (quit, tab switch, post) are
// handled first; zen action keys (i/f/u) act on the selected zen when the
// Zens tab is active; printable keys go to the compose input; navigation
// keys go to the active panel.
func (m model) handleKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit
	case "enter":
		// Enter posts when the compose line has text.
		if text := m.compose.value(); text != "" {
			m.compose.reset()
			return m, postCmd(m.socket, text)
		}
		return m.routeToPanel(msg)
	case "tab":
		m.activeTab = (m.activeTab + 1) % len(tabs)
		return m, nil
	case "shift+tab":
		m.activeTab = (m.activeTab - 1 + len(tabs)) % len(tabs)
		return m, nil
	case "esc":
		// esc clears a half-typed post, or quits when the line is empty.
		if m.compose.value() != "" {
			m.compose.reset()
			return m, nil
		}
		return m, tea.Quit
	}

	// On the Zens tab, i/f/u act on the selected zen.
	if m.activeTab == tabZens {
		if mm, cmd, handled := m.zenAction(msg); handled {
			return mm, cmd
		}
	}

	// Printable characters and editing keys go to the compose input.
	if isPrintable(msg) {
		var cmd tea.Cmd
		m.compose, cmd = m.compose.update(msg)
		return m, cmd
	}

	// Navigation keys route to the active panel.
	return m.routeToPanel(msg)
}

// zenAction handles i (info), f (follow), and u (unfollow) for the zen
// selected in the Zens list. It returns handled=true only when it consumed
// the key, so other keys fall through to compose/navigation.
func (m model) zenAction(msg tea.KeyPressMsg) (model, tea.Cmd, bool) {
	z, ok := m.zens.SelectedItem().(zenItem)
	if !ok || z.identity == "" {
		return m, nil, false
	}
	switch msg.String() {
	case "i":
		m.notice = fmt.Sprintf("%s  %s  %s  verified:%v", z.name, z.identity, z.status, z.verified)
		return m, nil, true
	case "f":
		if m.isFollowing(z.identity) {
			m.notice = "already following " + z.name
			return m, nil, true
		}
		return m, actionCmd("follow", m.socket, z.identity, followZen), true
	case "u":
		if !m.isFollowing(z.identity) {
			m.notice = "not following " + z.name
			return m, nil, true
		}
		return m, actionCmd("unfollow", m.socket, z.identity, unfollowZen), true
	}
	return m, nil, false
}

// isPrintable reports whether a key represents text input for the compose box.
func isPrintable(msg tea.KeyPressMsg) bool {
	// Editing keys that the textinput handles itself.
	switch msg.String() {
	case "backspace", "delete", "ctrl+w", "ctrl+u", "ctrl+a", "ctrl+e",
		"left", "right", "alt+backspace", "ctrl+left", "ctrl+right":
		return true
	}
	// Any key with printable text is a compose character.
	return msg.Text != ""
}

// routeToPanel forwards a key to whichever panel the active tab shows.
func (m model) routeToPanel(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch m.activeTab {
	case tabFeed:
		var cmd tea.Cmd
		m.feed, cmd = m.feed.update(msg)
		return m, cmd
	case tabZens:
		var cmd tea.Cmd
		m.zens, cmd = m.zens.Update(msg)
		return m, cmd
	case tabFollows:
		var cmd tea.Cmd
		m.follows, cmd = m.follows.Update(msg)
		return m, cmd
	case tabFollowers:
		var cmd tea.Cmd
		m.followers, cmd = m.followers.Update(msg)
		return m, cmd
	}
	return m, nil
}

// isFollowing reports whether the given identity is in the follow graph.
func (m model) isFollowing(identity string) bool {
	for _, it := range m.follows.Items() {
		if f, ok := it.(followItem); ok && f.identity == identity {
			return true
		}
	}
	return false
}
