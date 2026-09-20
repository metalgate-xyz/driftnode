package tui

import (
	"fmt"

	"charm.land/lipgloss/v2"
)

// renderZen renders one zen row as a single line, mirroring the feed's
// layout: the name in the accent color (falling back to the identity when no
// name is set), a verified check, the colored status word, and the identity
// in the muted color. When selected, each segment is re-styled (accent on a
// highlighted background) so ANSI resets don't defeat the highlight.
//
// Follows and followers are the same shape with no status and no verified
// flag, so they share this renderer: an empty status simply omits the status
// word, leaving name and identity.
func renderZen(z zen, selected bool) string {
	name := z.name
	if name == "" {
		name = z.identity
	}

	nameStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(accent))
	idStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(muted))
	if selected {
		nameStyle = nameStyle.Background(lipgloss.Color("#282a40"))
		idStyle = idStyle.Background(lipgloss.Color("#282a40")).Foreground(lipgloss.Color(accent))
	}

	check := ""
	if z.verified {
		check = lipgloss.NewStyle().Foreground(lipgloss.Color(good)).Render("✓ ")
	}
	title := nameStyle.Render(name)
	id := idStyle.Render(z.identity)

	if z.status == "" {
		return fmt.Sprintf("%s%s  %s", check, title, id)
	}
	status := badge(z.status).Render(fmt.Sprintf("%-11s", z.status))
	return fmt.Sprintf("%s%s  %s  %s", check, title, status, id)
}

// newZenList builds the scrollable list for the Zens tab.
func newZenList(w, h int) selectList[zen] {
	return newSelectList[zen](w, h, renderZen)
}
