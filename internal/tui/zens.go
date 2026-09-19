package tui

import (
	"fmt"
	"io"

	"charm.land/bubbles/v2/list"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// zenItem adapts a zen to the list item interface.
type zenItem struct {
	zen
}

func (z zenItem) FilterValue() string { return z.name + " " + z.identity }
func (z zenItem) Title() string       { return z.name }
func (z zenItem) Description() string { return fmt.Sprintf("%s  %s", z.status, z.identity) }

// zenDelegate renders zen rows with the name as title and a colored status
// word plus the identity as description.
type zenDelegate struct {
	styles list.DefaultItemStyles
}

func newZenDelegate() zenDelegate {
	s := list.NewDefaultItemStyles(true)
	s.SelectedTitle = s.SelectedTitle.BorderForeground(lipgloss.Color(accent)).Foreground(lipgloss.Color(accent))
	s.SelectedDesc = s.SelectedDesc.BorderForeground(lipgloss.Color(accent)).Foreground(lipgloss.Color(muted))
	return zenDelegate{styles: s}
}

func (d zenDelegate) Height() int                             { return 2 }
func (d zenDelegate) Spacing() int                            { return 1 }
func (d zenDelegate) Update(_ tea.Msg, _ *list.Model) tea.Cmd { return nil }

func (d zenDelegate) Render(w io.Writer, m list.Model, index int, item list.Item) {
	z, ok := item.(zenItem)
	if !ok {
		return
	}
	title := z.Title()
	if z.verified {
		title = lipgloss.NewStyle().Foreground(lipgloss.Color(good)).Render("✓ ") + title
	}
	desc := badge(z.status).Render(fmt.Sprintf("%-11s", z.status)) + "  " + z.identity

	isSelected := index == m.Index()
	matchedRunes := m.MatchesForItem(index)
	if isSelected && m.FilterState() != list.Filtering {
		if len(matchedRunes) > 0 {
			unmatched := d.styles.SelectedTitle.Inline(true)
			matched := unmatched.Inherit(d.styles.FilterMatch)
			title = lipgloss.StyleRunes(title, matchedRunes, matched, unmatched)
		}
		title = d.styles.SelectedTitle.Render(title)
		desc = d.styles.SelectedDesc.Render(desc)
	} else {
		if len(matchedRunes) > 0 {
			unmatched := d.styles.NormalTitle.Inline(true)
			matched := unmatched.Inherit(d.styles.FilterMatch)
			title = lipgloss.StyleRunes(title, matchedRunes, matched, unmatched)
		}
		title = d.styles.NormalTitle.Render(title)
		desc = d.styles.NormalDesc.Render(desc)
	}
	fmt.Fprintf(w, "%s\n%s", title, desc)
}

// newZenList builds the list for the Zens tab.
func newZenList(w, h int) list.Model {
	l := list.New(nil, newZenDelegate(), w, h)
	l.Title = "Discovered Zens"
	l.SetShowTitle(true)
	l.SetShowFilter(true)
	l.SetShowStatusBar(false)
	l.SetShowPagination(true)
	l.SetShowHelp(false)
	l.DisableQuitKeybindings()
	return l
}
