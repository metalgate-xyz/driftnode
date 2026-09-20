package tui

import (
	"fmt"

	"charm.land/lipgloss/v2"
)

// renderZen renders one zen row. The selected row is tinted with the accent
// color like the old list delegate did; an unselected row is plain.
func renderZen(z zen, selected bool) string {
	title := z.name
	if z.verified {
		title = lipgloss.NewStyle().Foreground(lipgloss.Color(good)).Render("✓ ") + title
	}
	desc := badge(z.status).Render(fmt.Sprintf("%-11s", z.status)) + "  " + z.identity
	if selected {
		title = lipgloss.NewStyle().Foreground(lipgloss.Color(accent)).Bold(true).Render(title)
		desc = lipgloss.NewStyle().Foreground(lipgloss.Color(muted)).Render(desc)
	}
	return fmt.Sprintf("%s\n%s", title, desc)
}

// newZenList builds the scrollable list for the Zens tab.
func newZenList(w, h int) selectList[zen] {
	return newSelectList[zen](w, h, renderZen)
}
