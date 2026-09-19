package tui

import (
	"fmt"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"driftnode/internal/daemon"
)

// refreshInterval is how often the model polls the daemon.
const refreshInterval = 2 * time.Second

// feedPost is one post in the merged timeline.
type feedPost struct {
	author string
	name   string
	text   string
	age    string
}

// zen is a discovered/connected zen from the daemon.
type zen struct {
	name     string
	identity string
	id       string
	kind     string
	status   string
	verified bool
}

// identityEntry is one row of the Follows or Followers list.
type identityEntry struct {
	identity string
	name     string
}

// statusInfo is the daemon health snapshot.
type statusInfo struct {
	zens      int
	transport bool
	listen    string
	unlocked  bool
}

// feedCmd polls the daemon for feed, zens, follows, followers, and status in
// one round trip and bundles them as refreshMsg.
func feedCmd(socket string) tea.Cmd {
	return func() tea.Msg {
		var msg refreshMsg
		if r, err := daemon.SendRequest(socket, "feed", map[string]any{"limit": 200}); err == nil {
			msg.feed, msg.feedErr = parseFeed(r)
		} else {
			msg.feedErr = err
		}
		if r, err := daemon.SendRequest(socket, "zens", nil); err == nil {
			msg.zens, msg.zensErr = parseZens(r)
		} else {
			msg.zensErr = err
		}
		if r, err := daemon.SendRequest(socket, "follows", nil); err == nil {
			msg.follows, msg.followsErr = parseIdentities(r, "follows")
		} else {
			msg.followsErr = err
		}
		if r, err := daemon.SendRequest(socket, "followers", nil); err == nil {
			msg.followers, msg.followersErr = parseIdentities(r, "followers")
		} else {
			msg.followersErr = err
		}
		if r, err := daemon.SendRequest(socket, "status", nil); err == nil {
			msg.status, msg.statusErr = parseStatus(r)
		} else {
			msg.statusErr = err
		}
		return msg
	}
}

// tickCmd waits refreshInterval before the next poll, so the UI refreshes on a
// calm cadence instead of hot-looping.
func tickCmd(socket string) tea.Cmd {
	return tea.Tick(refreshInterval, func(time.Time) tea.Msg {
		return tickMsg{}
	})
}

type tickMsg struct{}

type refreshMsg struct {
	feed         []feedPost
	feedErr      error
	zens         []zen
	zensErr      error
	follows      []identityEntry
	followsErr   error
	followers    []identityEntry
	followersErr error
	status       statusInfo
	statusErr    error
}

func parseFeed(r *daemon.Response) ([]feedPost, error) {
	items, ok := r.Result.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected feed response")
	}
	out := make([]feedPost, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		ts, _ := m["timestamp"].(float64)
		name, _ := m["name"].(string)
		out = append(out, feedPost{
			name:   name,
			author: fmt.Sprintf("%v", m["author"]),
			text:   fmt.Sprintf("%v", m["text"]),
			age:    ageOf(int64(ts)),
		})
	}
	return out, nil
}

func parseZens(r *daemon.Response) ([]zen, error) {
	zens, ok := r.Result.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected zens response")
	}
	out := make([]zen, 0, len(zens))
	for _, p := range zens {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		name, _ := pm["name"].(string)
		identity, _ := pm["identity"].(string)
		display := name
		if display == "" && identity != "" {
			display = shortID(identity)
		}
		if display == "" {
			display = shortID(fmt.Sprintf("%v", pm["id"]))
		}
		out = append(out, zen{
			name:     display,
			identity: identity,
			id:       fmt.Sprintf("%v", pm["id"]),
			kind:     fmt.Sprintf("%v", pm["kind"]),
			status:   fmt.Sprintf("%v", pm["status"]),
			verified: pm["verified"] == true,
		})
	}
	// The daemon builds the zen list from a map, so sort to keep it stable.
	slices.SortFunc(out, func(a, b zen) int { return strings.Compare(a.name, b.name) })
	return out, nil
}

func parseIdentities(r *daemon.Response, key string) ([]identityEntry, error) {
	m, ok := r.Result.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unexpected %s response", key)
	}
	items, _ := m[key].([]any)
	out := make([]identityEntry, 0, len(items))
	for _, it := range items {
		entry, ok := it.(map[string]any)
		if !ok {
			continue
		}
		id, _ := entry["identity"].(string)
		name, _ := entry["name"].(string)
		if name == "" {
			name = shortID(id)
		}
		out = append(out, identityEntry{identity: id, name: name})
	}
	return out, nil
}

func parseStatus(r *daemon.Response) (statusInfo, error) {
	m, ok := r.Result.(map[string]any)
	if !ok {
		return statusInfo{}, fmt.Errorf("unexpected status response")
	}
	zens, _ := m["zens"].(float64)
	return statusInfo{
		zens:      int(zens),
		transport: m["transport"] == true,
		unlocked:  m["unlocked"] == true,
		listen:    fmt.Sprintf("%v", m["listen_addr"]),
	}, nil
}

// actionResultMsg is the outcome of a follow/unfollow/info operation.
type actionResultMsg struct {
	notice string
	err    error
}

// actionCmd runs a follow/unfollow RPC and reports the result.
func actionCmd(label, socket, target string, fn func(string, string) (string, error)) tea.Cmd {
	return func() tea.Msg {
		notice, err := fn(socket, target)
		if err != nil {
			return actionResultMsg{err: err}
		}
		return actionResultMsg{notice: label + ": " + notice}
	}
}

func followZen(socket, target string) (string, error) {
	resp, err := daemon.SendRequest(socket, "follow", map[string]any{"target": target})
	if err != nil {
		return "", err
	}
	if m, ok := resp.Result.(map[string]any); ok {
		if id, ok := m["followed"].(string); ok {
			return id, nil
		}
	}
	return "followed", nil
}

func unfollowZen(socket, target string) (string, error) {
	resp, err := daemon.SendRequest(socket, "unfollow", map[string]any{"target": target})
	if err != nil {
		return "", err
	}
	if m, ok := resp.Result.(map[string]any); ok {
		if id, ok := m["unfollowed"].(string); ok {
			return id, nil
		}
	}
	return "unfollowed", nil
}

// postCmd sends a post to the daemon. The daemon holds the unlocked key.
func postCmd(socket, text string) tea.Cmd {
	return func() tea.Msg {
		resp, err := daemon.SendRequest(socket, "post", map[string]any{"text": text})
		if err != nil {
			return actionResultMsg{err: err}
		}
		if m, ok := resp.Result.(map[string]any); ok {
			return actionResultMsg{notice: "posted " + fmt.Sprintf("%v", m["event_id"])}
		}
		return actionResultMsg{notice: "posted"}
	}
}

// shortID keeps the TUI readable by truncating long driftnode identities.
func shortID(id string) string {
	const prefix = "driftnode:"
	if strings.HasPrefix(id, prefix) {
		return id
	}
	return id
}

// ageOf renders the age of a post from a unix-nanosecond timestamp.
func ageOf(ts int64) string {
	if ts == 0 {
		return "?"
	}
	d := time.Since(time.Unix(0, ts))
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

func padRight(s string, n int) string {
	if lipgloss.Width(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-lipgloss.Width(s))
}

// truncate caps s to n visible cells, appending an ellipsis if it was longer.
func truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if lipgloss.Width(s) <= n {
		return s
	}
	if n == 1 {
		return "…"
	}
	return s[:n-1] + "…"
}
