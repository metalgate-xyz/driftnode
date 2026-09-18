package tui

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"driftnode/internal/daemon"
)

// refreshInterval is how often the model polls the daemon for feed/peers/status.
const refreshInterval = 2 * time.Second

type model struct {
	socket   string
	identity string
	width    int
	height   int

	feed     []feedItem
	peers    []peerInfo
	status   statusInfo
	logLines []string
	cursor   int

	composing  bool
	composeBuf strings.Builder
	err        error
}

type feedItem struct {
	author string
	text   string
	age    string
}

type peerInfo struct {
	id     string
	kind   string
	status string
}

type statusInfo struct {
	syncOK    bool
	syncAgo   string
	syncRound int
	bootstrap string
	unlocked  bool
	peers     int
	transport bool
}

func newModel(socket, identity string) model {
	return model{
		socket:   socket,
		identity: identity,
		status:   statusInfo{bootstrap: "not verified"},
	}
}

func (m model) Init() tea.Cmd {
	return refresh(m.socket)
}

// refresh is a tea.Cmd that queries the daemon for feed, peers, and status
// and returns a refreshMsg bundling all three.
func refresh(socket string) tea.Cmd {
	return func() tea.Msg {
		var msg refreshMsg
		if resp, err := daemon.SendRequest(socket, "feed", map[string]any{"limit": 100}); err == nil {
			msg.feed, msg.feedErr = parseFeed(resp)
		} else {
			msg.feedErr = err
		}
		if resp, err := daemon.SendRequest(socket, "peers", nil); err == nil {
			msg.peers, msg.peersErr = parsePeers(resp)
		} else {
			msg.peersErr = err
		}
		if resp, err := daemon.SendRequest(socket, "status", nil); err == nil {
			msg.status, msg.statusErr = parseStatus(resp)
		} else {
			msg.statusErr = err
		}
		msg.time = time.Now()
		return msg
	}
}

type refreshMsg struct {
	feed      []feedItem
	feedErr   error
	peers     []peerInfo
	peersErr  error
	status    statusInfo
	statusErr error
	time      time.Time
}

func parseFeed(resp *daemon.Response) ([]feedItem, error) {
	items, ok := resp.Result.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected feed response")
	}
	out := make([]feedItem, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		ts, _ := m["timestamp"].(float64)
		out = append(out, feedItem{
			author: shortID(fmt.Sprintf("%v", m["author"])),
			text:   fmt.Sprintf("%v", m["text"]),
			age:    ageOf(int64(ts)),
		})
	}
	return out, nil
}

func parsePeers(resp *daemon.Response) ([]peerInfo, error) {
	peers, ok := resp.Result.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected peers response")
	}
	out := make([]peerInfo, 0, len(peers))
	for _, p := range peers {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, peerInfo{
			id:     fmt.Sprintf("%v", pm["id"]),
			kind:   fmt.Sprintf("%v", pm["kind"]),
			status: fmt.Sprintf("%v", pm["status"]),
		})
	}
	return out, nil
}

func parseStatus(resp *daemon.Response) (statusInfo, error) {
	m, ok := resp.Result.(map[string]any)
	if !ok {
		return statusInfo{}, fmt.Errorf("unexpected status response")
	}
	peers, _ := m["peers"].(float64)
	transport, _ := m["transport"].(bool)
	unlocked, _ := m["unlocked"].(bool)
	return statusInfo{
		bootstrap: "verified",
		peers:     int(peers),
		transport: transport,
		unlocked:  unlocked,
	}, nil
}

// postCmd is a tea.Cmd that sends a post to the daemon. The daemon holds the
// unlocked key, so no passphrase is needed.
func postCmd(socket, text string) tea.Cmd {
	return func() tea.Msg {
		resp, err := daemon.SendRequest(socket, "post", map[string]any{"text": text})
		if err != nil {
			return postResultMsg{err: err}
		}
		m, ok := resp.Result.(map[string]any)
		if !ok {
			return postResultMsg{err: fmt.Errorf("unexpected post response: %v", resp.Result)}
		}
		return postResultMsg{id: fmt.Sprintf("%v", m["event_id"])}
	}
}

type postResultMsg struct {
	id  string
	err error
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case refreshMsg:
		if msg.feedErr == nil {
			m.feed = msg.feed
		}
		if msg.peersErr == nil {
			m.peers = msg.peers
		}
		if msg.statusErr == nil {
			m.status = msg.status
		}
		return m, refresh(m.socket)
	case postResultMsg:
		if msg.err != nil {
			m.err = msg.err
			m.logLines = append([]string{"post error: " + msg.err.Error()}, m.logLines...)
		} else {
			m.logLines = append([]string{"posted " + msg.id}, m.logLines...)
		}
		if len(m.logLines) > 10 {
			m.logLines = m.logLines[:10]
		}
		// An immediate refresh picks up the new post.
		return m, refresh(m.socket)
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "j", "down":
			if m.cursor < len(m.feed)-1 {
				m.cursor++
			}
		case "k", "up":
			if m.cursor > 0 {
				m.cursor--
			}
		case "i":
			if !m.composing {
				m.composing = true
				m.err = nil
			}
		case "esc":
			m.composing = false
			m.composeBuf.Reset()
		case "enter":
			if m.composing {
				text := m.composeBuf.String()
				m.composing = false
				m.composeBuf.Reset()
				if text != "" {
					return m, postCmd(m.socket, text)
				}
			}
		default:
			if m.composing {
				m.composeBuf.WriteString(msg.String())
			}
		}
	}
	return m, nil
}

func (m model) View() string {
	if m.width == 0 {
		return "Loading..."
	}

	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 1)

	titleStyle := lipgloss.NewStyle().Bold(true)

	// Feed panel
	feedW := m.width * 2 / 3
	peerW := m.width - feedW - 2

	feedTitle := titleStyle.Render("driftnode - you: " + shortID(m.identity))
	var feedLines []string
	for i, item := range m.feed {
		marker := "  "
		if i == m.cursor {
			marker = "> "
		}
		feedLines = append(feedLines, marker+item.author+"> "+item.text+"  "+item.age)
	}
	if len(feedLines) == 0 {
		feedLines = []string{"(no posts yet)"}
	}

	// Peers panel
	peersTitle := titleStyle.Render("Peers")
	var peerLines []string
	for _, p := range m.peers {
		peerLines = append(peerLines, p.status+" "+p.id+" ("+p.kind+")")
	}
	if len(peerLines) == 0 {
		peerLines = []string{"(no peers)"}
	}

	// Status panel
	statusTitle := titleStyle.Render("Status")
	syncStr := "not synced"
	if m.status.syncOK {
		syncStr = "ok, " + m.status.syncAgo + " ago"
	}
	statusLines := []string{
		"sync: " + syncStr,
		"peers: " + fmt.Sprintf("%d", m.status.peers),
		"transport: " + boolStr(m.status.transport),
		"key: " + keyStr(m.status.unlocked),
		"bootstrap: " + m.status.bootstrap,
	}

	peersAndStatus := lipgloss.JoinVertical(lipgloss.Left,
		borderStyle.Width(peerW).Render(peersTitle+"\n"+strings.Join(peerLines, "\n")),
		borderStyle.Width(peerW).Render(statusTitle+"\n"+strings.Join(statusLines, "\n")),
	)

	topRow := lipgloss.JoinHorizontal(lipgloss.Top,
		borderStyle.Width(feedW).Render(feedTitle+"\n"+strings.Join(feedLines, "\n")),
		peersAndStatus,
	)

	// Compose bar
	composeStr := "> " + m.composeBuf.String()
	if m.composing {
		composeStr += "_"
	}
	composeBar := borderStyle.Width(m.width - 2).Render(composeStr)

	// Log tail
	logTitle := titleStyle.Render("log")
	logContent := "(no logs)"
	if len(m.logLines) > 0 {
		logContent = strings.Join(m.logLines, "\n")
	}
	logPanel := borderStyle.Width(m.width - 2).Render(logTitle + "\n" + logContent)

	return lipgloss.JoinVertical(lipgloss.Left, topRow, composeBar, logPanel)
}

// Run starts the TUI as a client of the daemon control socket. The daemon
// must be running and unlocked; Run does not prompt for a passphrase.
func Run(socket, identity string) error {
	m := newModel(socket, identity)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func shortID(id string) string {
	if len(id) < 20 {
		return id
	}
	return id[:12] + "..." + id[len(id)-4:]
}

func ageOf(ts int64) string {
	if ts == 0 {
		return "?"
	}
	d := time.Since(time.Unix(ts, 0))
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

func boolStr(b bool) string {
	if b {
		return "up"
	}
	return "down"
}

func keyStr(unlocked bool) string {
	if unlocked {
		return "unlocked"
	}
	return "locked"
}

// Compile-time check: keep json import for future RPC param extension.
var _ = json.Marshal
