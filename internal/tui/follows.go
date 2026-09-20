package tui

// newFollowList builds the scrollable list for a Follows or Followers tab.
// The daemon returns only name and identity for follows, so they share
// renderZen: an empty status omits the status word, leaving name and identity.
func newFollowList(w, h int) selectList[zen] {
	return newSelectList[zen](w, h, renderZen)
}
