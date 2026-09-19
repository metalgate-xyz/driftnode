package tui

import (
	"fmt"

	tea "charm.land/bubbletea/v2"
)

// handleKey routes a key press. Global keys (quit, tab switch, post) are
// handled first; printable keys go to the compose input; navigation keys go
// to the active panel.
func (m model) handleKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit
	case "enter":
		// Enter posts when the compose line has text; otherwise, on the
		// Zens tab, it opens the action menu.
		if text := m.compose.value(); text != "" {
			m.compose.reset()
			return m, postCmd(m.socket, text)
		}
		if m.mode == modeMenu {
			return m.handleMenuKey(msg)
		}
		if m.activeTab == tabZens {
			if _, ok := m.zens.SelectedItem().(zenItem); ok {
				m.mode = modeMenu
				m.menu = m.buildMenu()
			}
		}
		return m, nil
	case "tab":
		m.activeTab = (m.activeTab + 1) % len(tabs)
		m.mode = modeBrowse
		return m, nil
	case "shift+tab":
		m.activeTab = (m.activeTab - 1 + len(tabs)) % len(tabs)
		m.mode = modeBrowse
		return m, nil
	case "esc":
		if m.mode == modeMenu {
			m.mode = modeBrowse
			return m, nil
		}
		// esc clears a half-typed post, or quits when the line is empty.
		if m.compose.value() != "" {
			m.compose.reset()
			return m, nil
		}
		return m, tea.Quit
	}

	// In the zen action menu, number keys run the chosen operation.
	if m.mode == modeMenu {
		return m.handleMenuKey(msg)
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

// handleMenuKey interprets a key while the zen action menu is open.
func (m model) handleMenuKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "ctrl+c":
		m.mode = modeBrowse
		return m, nil
	}
	// 1..9 selects a menu entry.
	if len(msg.Text) == 1 && msg.Text[0] >= '1' && msg.Text[0] <= '9' {
		idx := int(msg.Text[0] - '1')
		if idx < len(m.menu) {
			return m.menu[idx].run(m)
		}
	}
	return m, nil
}

// buildMenu assembles the follow/unfollow/info operations for the selected zen.
func (m model) buildMenu() []menuItem {
	z, ok := m.zens.SelectedItem().(zenItem)
	if !ok {
		return nil
	}
	target := z.identity
	if target == "" {
		return []menuItem{{label: "no identity to act on"}}
	}
	items := []menuItem{
		{
			label: "info",
			run: func(mod model) (tea.Model, tea.Cmd) {
				return mod.info(z.zen), nil
			},
		},
	}
	if m.isFollowing(target) {
		items = append(items, menuItem{
			label: "unfollow",
			run: func(mod model) (tea.Model, tea.Cmd) {
				mod.mode = modeBrowse
				return mod, actionCmd("unfollow", mod.socket, target, unfollowZen)
			},
		})
	} else {
		items = append(items, menuItem{
			label: "follow",
			run: func(mod model) (tea.Model, tea.Cmd) {
				mod.mode = modeBrowse
				return mod, actionCmd("follow", mod.socket, target, followZen)
			},
		})
	}
	return items
}

// info sets a notice describing the selected zen and closes the menu.
func (m model) info(z zen) tea.Model {
	m.mode = modeBrowse
	m.notice = fmt.Sprintf("%s  %s  %s  verified:%v", z.name, z.identity, z.status, z.verified)
	return m
}

// isFollowing reports whether the selected zen is in the follow graph.
func (m model) isFollowing(identity string) bool {
	for _, it := range m.follows.Items() {
		if f, ok := it.(followItem); ok && f.identity == identity {
			return true
		}
	}
	return false
}
