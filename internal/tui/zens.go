package tui

import (
	"fmt"

	"charm.land/lipgloss/v2"
	"driftnode/internal/core"
)

// renderZen renders one zen row as a single line, mirroring the feed's
// layout: the name in the accent color (falling back to the identity when no
// name is set), a verified marker, a pin marker (follows only), the colored
// status word, and the identity in the muted color. When selected, each
// segment is re-styled (accent on a highlighted background) so ANSI resets
// don't defeat the highlight.
//
// Follows and followers share this renderer. The zens tab never sets the
// pinned flag (a pin is a follow-edge property), so the marker appears only
// on the follows list. An empty status simply omits the status word.
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

	marks := ""
	if z.verified {
		marks += lipgloss.NewStyle().Foreground(lipgloss.Color(good)).Render(core.GlyphVerified + " ")
	}
	if z.pinned {
		marks += lipgloss.NewStyle().Foreground(lipgloss.Color(accent)).Render(core.GlyphPin + " ")
	}
	title := nameStyle.Render(name)
	id := idStyle.Render(z.identity)

	if z.status == "" {
		return fmt.Sprintf("%s%s  %s", marks, title, id)
	}
	status := badge(z.status).Render(fmt.Sprintf("%-11s", z.status))
	return fmt.Sprintf("%s%s  %s  %s", marks, title, status, id)
}

// newZenList builds the scrollable list for the Zens tab. Zens are keyed by
// their token (id), matching the daemon's zens upsert/remove diffs.
func newZenList(w, h int) selectList[zen] {
	return newSelectList[zen](w, h, renderZen, func(z zen) string { return z.id })
}
