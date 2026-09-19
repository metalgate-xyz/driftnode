package tui

import (
	"fmt"
	"io"

	"charm.land/bubbles/v2/list"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// followItem adapts an identity entry to the list item interface.
type followItem struct {
	identityEntry
}

func (f followItem) FilterValue() string { return f.name + " " + f.identity }
func (f followItem) Title() string       { return f.name }
func (f followItem) Description() string { return f.identity }

// followDelegate renders an identity row with the name as title and the full
// identity as description.
type followDelegate struct {
	styles list.DefaultItemStyles
}

func newFollowDelegate() followDelegate {
	s := list.NewDefaultItemStyles(true)
	s.SelectedTitle = s.SelectedTitle.BorderForeground(lipgloss.Color(accent)).Foreground(lipgloss.Color(accent))
	s.SelectedDesc = s.SelectedDesc.BorderForeground(lipgloss.Color(accent)).Foreground(lipgloss.Color(muted))
	return followDelegate{styles: s}
}

func (d followDelegate) Height() int                             { return 2 }
func (d followDelegate) Spacing() int                            { return 1 }
func (d followDelegate) Update(_ tea.Msg, _ *list.Model) tea.Cmd { return nil }

func (d followDelegate) Render(w io.Writer, m list.Model, index int, item list.Item) {
	f, ok := item.(followItem)
	if !ok {
		return
	}
	title := f.Title()
	desc := f.Description()

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

// newFollowList builds the list for a Follows or Followers tab.
func newFollowList(title string, w, h int) list.Model {
	l := list.New(nil, newFollowDelegate(), w, h)
	l.Title = title
	l.SetShowTitle(true)
	l.SetShowFilter(true)
	l.SetShowStatusBar(false)
	l.SetShowPagination(true)
	l.SetShowHelp(false)
	l.DisableQuitKeybindings()
	return l
}

// setFollowItems replaces the items in a follows/followers list.
func setFollowItems(l *list.Model, entries []identityEntry) {
	items := make([]list.Item, 0, len(entries))
	for _, e := range entries {
		items = append(items, followItem{e})
	}
	_ = l.SetItems(items)
}
