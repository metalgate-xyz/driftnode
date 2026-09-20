package tui

import (
	"fmt"

	"charm.land/lipgloss/v2"
)

// renderFollow renders one follows/followers row: the display name as title
// and the full identity as description. The selected row is tinted with the
// accent color.
func renderFollow(e identityEntry, selected bool) string {
	title := e.name
	desc := e.identity
	if selected {
		title = lipgloss.NewStyle().Foreground(lipgloss.Color(accent)).Bold(true).Render(title)
		desc = lipgloss.NewStyle().Foreground(lipgloss.Color(muted)).Render(desc)
	}
	return fmt.Sprintf("%s\n%s", title, desc)
}

// newFollowList builds the scrollable list for a Follows or Followers tab.
func newFollowList(w, h int) selectList[identityEntry] {
	return newSelectList[identityEntry](w, h, renderFollow)
}
