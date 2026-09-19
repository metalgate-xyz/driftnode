package tui

// A tab is one pane of the UI. The model holds an active tab index and only
// renders the panel for that tab, so each tab owns the full window and shows
// exactly one thing.
type tab struct {
	name string
}

var tabs = []tab{
	{name: "Feed"},
	{name: "Zens"},
	{name: "Follows"},
	{name: "Followers"},
}

const (
	tabFeed = iota
	tabZens
	tabFollows
	tabFollowers
)
