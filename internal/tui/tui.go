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
//
// Each region is clipped to a fixed row budget with MaxHeight so the five
// regions always sum to the window height: an over-tall panel scrolls inside
// its own box instead of pushing the compose and status lines off-screen.
func (m model) View() tea.View {
	if m.width == 0 {
		return tea.NewView("Loading...")
	}

	title := theme.titleBar.
		Width(m.width).
		MaxHeight(1).
		Render("driftnode " + theme.identity.Render(shortID(m.identity)))

	tabs := m.renderTabs()
	panel := m.renderActivePanel()
	compose := m.compose.render(m.width)
	status := m.renderStatus()

	body := lipgloss.JoinVertical(lipgloss.Top,
		title, tabs, panel, compose, status,
	)

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
	return theme.tabBar.Width(m.width).MaxHeight(1).Render(row)
}

// tabAt maps a screen X coordinate to the tab index at that column, or -1 if
// the column is outside any tab label. It mirrors renderTabs: the bar has one
// cell of left padding, each label carries two cells of padding on both
// sides, and labels are separated by two spaces.
func (m model) tabAt(x int) int {
	sep := lipgloss.Width(theme.tabBar.Render("  "))
	x -= theme.tabBar.GetHorizontalFrameSize() / 2
	for i, t := range tabs {
		var label string
		if i == m.activeTab {
			label = theme.tabActive.Render(t.name)
		} else {
			label = theme.tab.Render(t.name)
		}
		w := lipgloss.Width(label)
		if x >= 0 && x < w {
			return i
		}
		x -= w + sep
	}
	return -1
}

// renderActivePanel returns the panel for the current tab, bordered and sized
// to fill the window. MaxHeight clips the inner content to the panel's row
// budget so a panel that renders taller than its allotment scrolls inside
// its own box instead of overflowing the layout.
func (m model) renderActivePanel() string {
	var content string
	switch m.activeTab {
	case tabFeed:
		content = m.feed.render()
	case tabZens:
		content = m.zens.view()
	case tabFollows:
		content = m.follows.view()
	case tabFollowers:
		content = m.followers.view()
	}
	return theme.panel.
		Width(m.width).
		MaxHeight(panelHeight(m.height)).
		Render(content)
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
		right = theme.hint.Render("tab: switch  enter: post  i/f/u: zen  esc: quit")
	} else {
		right = theme.notice.Render(right)
	}
	return theme.statusBar.Width(m.width).MaxHeight(1).Render(left + "  " + right)
}

// Run starts the TUI as a client of the daemon control socket. The daemon must
// be running and unlocked; Run does not prompt for a passphrase.
func Run(socket, identity string) error {
	p := tea.NewProgram(newModel(socket, identity))
	_, err := p.Run()
	return err
}
