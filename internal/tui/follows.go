package tui

// newFollowList builds the scrollable list for a Follows or Followers tab.
// Follows carry verified and pinned flags; followers carry verified only.
// Both share renderZen: an empty status omits the status word. Follows and
// followers are keyed by identity, matching the daemon's add/remove diffs.
func newFollowList(w, h int) selectList[zen] {
	return newSelectList[zen](w, h, renderZen, func(z zen) string { return z.identity })
}
