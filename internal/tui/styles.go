package tui

import (
	"charm.land/lipgloss/v2"
)

// Color palette (Tokyo Night-ish). Named constants keep the panels and
// delegates in sync without scattering hex codes across files.
const (
	accent = "#7aa2f7"
	teal   = "#9ece6a"
	good   = "#9ece6a"
	warn   = "#e0af68"
	bad    = "#f7768e"
	muted  = "#6b6b85"
)

// theme groups the lipgloss styles used by the layout. Built once in
// newStyles and shared; panels read from it rather than building their own.
type styles struct {
	titleBar  lipgloss.Style
	identity  lipgloss.Style
	tabBar    lipgloss.Style
	tab       lipgloss.Style
	tabActive lipgloss.Style
	panel     lipgloss.Style
	hint      lipgloss.Style
	statusBar lipgloss.Style
	notice    lipgloss.Style
	menu      lipgloss.Style
	menuKey   lipgloss.Style
}

var theme = newStyles()

func newStyles() styles {
	border := lipgloss.RoundedBorder()
	panel := lipgloss.NewStyle().
		Border(border).
		BorderForeground(lipgloss.Color("#3b4159")).
		Padding(0, 1)

	return styles{
		titleBar: lipgloss.NewStyle().
			Background(lipgloss.Color("#1a1b26")).
			Foreground(lipgloss.Color("#c0caf5")).
			Padding(0, 1),
		identity: lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(accent)),
		tabBar: lipgloss.NewStyle().
			Foreground(lipgloss.Color(muted)).
			Padding(0, 1),
		tab: lipgloss.NewStyle().
			Padding(0, 2),
		tabActive: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color(accent)).
			Background(lipgloss.Color("#282a40")).
			Padding(0, 2),
		panel:     panel,
		hint:      lipgloss.NewStyle().Foreground(lipgloss.Color(muted)).Italic(true),
		statusBar: lipgloss.NewStyle().Padding(0, 1),
		notice: lipgloss.NewStyle().
			Foreground(lipgloss.Color(warn)).
			Padding(0, 1),
		menu: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(accent)).
			Padding(1, 2),
		menuKey: lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(warn)),
	}
}

// badge returns the style for a zen connection status word.
func badge(status string) lipgloss.Style {
	switch status {
	case "connected":
		return lipgloss.NewStyle().Foreground(lipgloss.Color(good)).Bold(true)
	case "connecting":
		return lipgloss.NewStyle().Foreground(lipgloss.Color(warn))
	case "error":
		return lipgloss.NewStyle().Foreground(lipgloss.Color(bad)).Bold(true)
	default:
		return lipgloss.NewStyle().Foreground(lipgloss.Color(muted))
	}
}
