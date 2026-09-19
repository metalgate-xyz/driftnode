package tui

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// View composes the title bar, tab bar, the active panel, the compose line,
// and the status line. Only the active tab's panel is rendered, so each tab
// owns the full window and shows exactly one thing.
func (m model) View() tea.View {
	if m.width == 0 {
		return tea.NewView("Loading...")
	}

	title := theme.titleBar.Width(m.width).Render(
		"driftnode " + theme.identity.Render(shortID(m.identity)),
	)

	tabs := m.renderTabs()
	panel := m.renderActivePanel()
	compose := m.compose.render(m.width)
	status := m.renderStatus()

	body := lipgloss.JoinVertical(lipgloss.Top,
		title, tabs, panel, compose, status,
	)

	if m.mode == modeMenu {
		body = m.renderMenu(body)
	}

	v := tea.NewView(body)
	v.AltScreen = true
	v.MouseMode = tea.MouseModeCellMotion
	return v
}

// renderTabs draws the tab strip, highlighting the active tab.
func (m model) renderTabs() string {
	var labels []string
	for i, t := range tabs {
		if i == m.activeTab {
			labels = append(labels, theme.tabActive.Render(t.name))
		} else {
			labels = append(labels, theme.tab.Render(t.name))
		}
	}
	row := strings.Join(labels, theme.tabBar.Render("  "))
	return theme.tabBar.Width(m.width).Render(row)
}

// renderActivePanel returns the panel for the current tab, bordered and sized
// to fill the window.
func (m model) renderActivePanel() string {
	var content string
	switch m.activeTab {
	case tabFeed:
		content = m.feed.render()
	case tabZens:
		content = m.zens.View()
	case tabFollows:
		content = m.follows.View()
	case tabFollowers:
		content = m.followers.View()
	}
	return theme.panel.Width(m.width).Height(panelHeight(m.height)).Render(content)
}

// renderStatus draws the one-line status: transport, key, zens, and notice.
func (m model) renderStatus() string {
	transport := badge("down").Render("down")
	if m.status.transport {
		transport = badge("connected").Render("up")
	}
	key := badge("error").Render("locked")
	if m.status.unlocked {
		key = badge("connected").Render("unlocked")
	}
	left := fmt.Sprintf("transport %s  key %s  zens %d", transport, key, m.status.zens)
	right := m.notice
	if right == "" {
		right = theme.hint.Render("tab: switch  enter: post/act  esc: quit")
	} else {
		right = theme.notice.Render(right)
	}
	return theme.statusBar.Width(m.width).Render(left + "  " + right)
}

// renderMenu overlays the zen action menu on the body, centered.
func (m model) renderMenu(body string) string {
	rows := make([]string, 0, len(m.menu)+1)
	rows = append(rows, theme.identity.Render("zen actions"))
	for i, item := range m.menu {
		rows = append(rows, fmt.Sprintf("%s  %s",
			theme.menuKey.Render(fmt.Sprintf("%d", i+1)), item.label))
	}
	menu := theme.menu.Render(strings.Join(rows, "\n"))
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, menu,
		lipgloss.WithWhitespaceStyle(lipgloss.NewStyle().Background(lipgloss.Color("#1a1b26"))))
}

// Run starts the TUI as a client of the daemon control socket. The daemon must
// be running and unlocked; Run does not prompt for a passphrase.
func Run(socket, identity string) error {
	p := tea.NewProgram(newModel(socket, identity))
	_, err := p.Run()
	return err
}
