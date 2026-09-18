package tui

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"driftnode/internal/daemon"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// refreshInterval is how often the model polls the daemon for feed/peers/status.
const refreshInterval = 2 * time.Second

// focus identifies which widget receives text input.
type focus int

const (
	focusFeed focus = iota
	focusCompose
)

func (f focus) String() string {
	switch f {
	case focusCompose:
		return "compose"
	default:
		return "feed"
	}
}

type model struct {
	socket   string
	identity string
	width    int
	height   int

	feed   []feedItem
	peers  []peerInfo
	status statusInfo
	cursor int

	focus      focus
	composeBuf strings.Builder
	logLines   []string
	notice     string
	help       bool
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
	peers     int
	transport bool
	listen    string
	unlocked  bool
}

func newModel(socket, identity string) model {
	return model{
		socket:   socket,
		identity: identity,
		focus:    focusFeed,
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
			id:     shortID(fmt.Sprintf("%v", pm["id"])),
			kind:   fmt.Sprintf("%v", pm["kind"]),
			status: fmt.Sprintf("%v", pm["status"]),
		})
	}
	// The daemon builds the peer list from a map, so its order is
	// non-deterministic across refreshes. Sort to keep the pane stable.
	slices.SortFunc(out, func(a, b peerInfo) int {
		return strings.Compare(a.id, b.id)
	})
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
	listen, _ := m["listen_addr"].(string)
	return statusInfo{
		peers:     int(peers),
		transport: transport,
		listen:    listen,
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
	// The help overlay captures all keys until dismissed.
	if m.help {
		if _, ok := msg.(tea.KeyPressMsg); ok {
			m.help = false
		}
		return m, nil
	}

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case refreshMsg:
		if msg.feedErr == nil {
			m.feed = msg.feed
		} else {
			m.notice = "feed: " + msg.feedErr.Error()
		}
		if msg.peersErr == nil {
			m.peers = msg.peers
		}
		if msg.statusErr == nil {
			m.status = msg.status
		}
		if m.cursor > len(m.feed)-1 {
			m.cursor = max(len(m.feed)-1, 0)
		}
		return m, refresh(m.socket)
	case postResultMsg:
		if msg.err != nil {
			m.notice = "post error: " + msg.err.Error()
			m.logLines = append([]string{"error: " + msg.err.Error()}, m.logLines...)
		} else {
			m.notice = "posted " + msg.id
			m.logLines = append([]string{"posted " + msg.id}, m.logLines...)
		}
		if len(m.logLines) > 10 {
			m.logLines = m.logLines[:10]
		}
		return m, refresh(m.socket)
	case tea.KeyPressMsg:
		if m.focus == focusCompose {
			return m.handleComposeKey(msg)
		}
		return m.handleFeedKey(msg)
	}
	return m, nil
}

func (m model) handleFeedKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		return m, quitCmd
	case "tab":
		m.focus = focusCompose
		m.composeBuf.Reset()
		m.notice = ""
	case "j", "down":
		if m.cursor < len(m.feed)-1 {
			m.cursor++
		}
	case "k", "up":
		if m.cursor > 0 {
			m.cursor--
		}
	case "g", "home":
		m.cursor = 0
	case "G", "end":
		if len(m.feed) > 0 {
			m.cursor = len(m.feed) - 1
		}
	case "r":
		m.notice = "refreshing..."
		return m, refresh(m.socket)
	case "h", "?":
		m.help = true
	}
	return m, nil
}

func (m model) handleComposeKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.focus = focusFeed
		m.composeBuf.Reset()
		m.notice = ""
	case "tab":
		m.focus = focusFeed
		m.composeBuf.Reset()
		m.notice = ""
	case "enter":
		text := strings.TrimSpace(m.composeBuf.String())
		m.composeBuf.Reset()
		m.focus = focusFeed
		if text != "" {
			m.notice = "posting..."
			return m, postCmd(m.socket, text)
		}
		m.notice = ""
	case "backspace":
		cur := m.composeBuf.String()
		if len(cur) > 0 {
			m.composeBuf.Reset()
			m.composeBuf.WriteString(cur[:len(cur)-1])
		}
	default:
		if msg.Text != "" {
			m.composeBuf.WriteString(msg.Text)
		}
	}
	return m, nil
}

func (m model) View() tea.View {
	if m.width == 0 {
		return tea.NewView("Loading...")
	}

	styles := newStyles()
	layout := computeLayout(m.width, m.height)

	title := styles.titleBar.Width(m.width).Render(
		fmt.Sprintf("driftnode %s", styles.identity.Render(shortID(m.identity))),
	)

	// Panels fill their column width and are capped to the body height via
	// MaxHeight, so they take only the space they need and never overflow.
	feedBlock := renderFeed(m, styles, layout.feedW, layout.bodyRows)
	peersBlock := renderPeers(m, styles, layout.sideW, layout.bodyRows/2)
	statusBlock := renderStatus(m, styles, layout.sideW, layout.bodyRows/2)
	sideColumn := lipgloss.JoinVertical(lipgloss.Left, peersBlock, statusBlock)
	// Cap the stacked side column to the body height so it matches the feed.
	sideColumn = lipgloss.NewStyle().MaxHeight(layout.bodyRows).Render(sideColumn)
	body := lipgloss.JoinHorizontal(lipgloss.Top, feedBlock, sideColumn)

	composeBar := renderCompose(m, styles, m.width)
	logPanel := renderLog(m, styles, m.width, layout.logRows)

	content := lipgloss.JoinVertical(lipgloss.Left, title, body, composeBar, logPanel)

	if m.help {
		content = renderHelp(styles, m.width, m.height, content)
	}

	v := tea.NewView(content)
	v.AltScreen = true
	return v
}

// panelDims holds the column widths and the row budgets for the body and log
// regions. Panels fill their column via Width and are capped to a maximum
// total height via MaxHeight, so content sizes itself and never overflows.
type panelDims struct {
	feedW    int
	sideW    int
	bodyRows int // max terminal rows for the feed / side-column region
	logRows  int // max terminal rows for the log panel
}

// computeLayout divides the terminal into widths and row budgets. The body
// region (feed + side column) and the log panel each get a max height; panels
// within them fill their column width and cap their height to fit.
func computeLayout(w, h int) panelDims {
	const sideMin = 36
	const sideMax = 56
	const logLines = 6
	const borderRows = 2

	sideW := w / 3
	if sideW < sideMin {
		sideW = sideMin
	}
	if sideW > sideMax {
		sideW = sideMax
	}
	if sideW > w-40 {
		sideW = max(w-40, 20)
	}

	// Title (1) + compose bar (1 content + 2 border) are fixed.
	fixedRows := 1 + (1 + borderRows)
	// Log panel: 1 head + logLines content + 2 border.
	logRows := 1 + logLines + borderRows
	bodyRows := h - fixedRows - logRows
	if bodyRows < 10 {
		bodyRows = 10
	}

	return panelDims{
		feedW:    w - sideW,
		sideW:    sideW,
		bodyRows: bodyRows,
		logRows:  logRows,
	}
}

type styles struct {
	titleBar  lipgloss.Style
	identity  lipgloss.Style
	panelHead lipgloss.Style

	panel lipgloss.Style

	author lipgloss.Style
	age   lipgloss.Style
	cursor lipgloss.Style

	peerStatus map[string]lipgloss.Style

	compose      lipgloss.Style
	composeLabel lipgloss.Style
	composeFocus lipgloss.Style
	composeDim   lipgloss.Style

	logPanel lipgloss.Style
	logHead  lipgloss.Style

	dim    lipgloss.Style
	good   lipgloss.Style
	bad    lipgloss.Style
	warn   lipgloss.Style
	accent lipgloss.Style

	helpBorder lipgloss.Style
	helpTitle  lipgloss.Style
	helpKey    lipgloss.Style
	helpDesc   lipgloss.Style
}

func newStyles() styles {
	border := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#5b5b7a")).
		Padding(0, 1)

	head := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#c8c8e0"))

	muted := lipgloss.Color("#6b6b85")
	accent := lipgloss.Color("#7aa2f7")
	good := lipgloss.Color("#9ece6a")
	bad := lipgloss.Color("#f7768e")
	warn := lipgloss.Color("#e0af68")

	peerStatus := map[string]lipgloss.Style{
		"connected":  lipgloss.NewStyle().Foreground(good).Bold(true),
		"connecting": lipgloss.NewStyle().Foreground(warn),
		"error":      lipgloss.NewStyle().Foreground(bad).Bold(true),
	}

	return styles{
		titleBar: lipgloss.NewStyle().
			Background(lipgloss.Color("#1a1b26")).
			Foreground(lipgloss.Color("#c0caf5")).
			Padding(0, 1),
		identity:  lipgloss.NewStyle().Bold(true).Foreground(accent),
		panelHead:  head,
		panel:      border,

		author: lipgloss.NewStyle().Bold(true).Foreground(accent),
		age:    lipgloss.NewStyle().Foreground(muted),
		cursor: lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#e0af68")),

		peerStatus: peerStatus,

		compose:      border,
		composeLabel: lipgloss.NewStyle().Bold(true).Foreground(muted),
		composeFocus: lipgloss.NewStyle().Bold(true).Foreground(good),
		composeDim:   lipgloss.NewStyle().Foreground(muted),

		logPanel: border,
		logHead:  head,

		dim:    lipgloss.NewStyle().Foreground(muted),
		good:   lipgloss.NewStyle().Foreground(good),
		bad:    lipgloss.NewStyle().Foreground(bad),
		warn:   lipgloss.NewStyle().Foreground(warn),
		accent: lipgloss.NewStyle().Foreground(accent),

		helpBorder: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(accent).
			Padding(1, 2),
		helpTitle: lipgloss.NewStyle().Bold(true).Foreground(accent),
		helpKey:   lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#e0af68")),
		helpDesc:  lipgloss.NewStyle().Foreground(lipgloss.Color("#c0caf5")),
	}
}

func renderFeed(m model, s styles, w, maxH int) string {
	head := s.panelHead.Render("Feed")
	var lines []string
	if len(m.feed) == 0 {
		lines = []string{s.dim.Render("(no posts yet)")}
	}
	// Size the author column to the widest author so rows never wrap on it.
	authorCol := len("author")
	for _, item := range m.feed {
		if len(item.author) > authorCol {
			authorCol = len(item.author)
		}
	}
	const ageCol = 6
	// Content width inside the panel: total minus 2 border, 2 padding.
	contentW := w - 2 - 2
	textW := contentW - 1 /*marker*/ - 4 /*gutters*/ - authorCol - ageCol
	if textW < 8 {
		textW = 8
	}
	for i, item := range m.feed {
		marker := " "
		if i == m.cursor && m.focus == focusFeed {
			marker = s.cursor.Render(">")
		}
		author := s.author.Render(padRight(item.author, authorCol))
		age := s.age.Render(padRight(item.age, ageCol))
		text := truncate(item.text, textW)
		lines = append(lines, fmt.Sprintf("%s %s %s  %s",
			marker,
			author,
			padRight(text, textW),
			age,
		))
	}
	body := strings.Join(lines, "\n")
	return s.panel.Width(w).MaxHeight(maxH).Render(head + "\n" + body)
}

func renderPeers(m model, s styles, w, maxH int) string {
	head := s.panelHead.Render("Peers")
	var lines []string
	if len(m.peers) == 0 {
		lines = []string{s.dim.Render("(no peers)")}
	}
	// Content width for the id field: panel width minus borders, padding,
	// the status column, the kind column, and the two separating spaces.
	const statusCol = 11
	contentW := w - 2 - 2
	for _, p := range m.peers {
		stStyle, ok := s.peerStatus[p.status]
		if !ok {
			stStyle = s.dim
		}
		kindW := len(p.kind)
		idW := contentW - statusCol - kindW - 2
		if idW < 8 {
			idW = 8
		}
		st := stStyle.Render(padRight(p.status, statusCol))
		kind := s.dim.Render(p.kind)
		id := s.accent.Render(truncate(p.id, idW))
		lines = append(lines, fmt.Sprintf("%s %s %s", st, kind, id))
	}
	body := strings.Join(lines, "\n")
	return s.panel.Width(w).MaxHeight(maxH).Render(head + "\n" + body)
}

func renderStatus(m model, s styles, w, maxH int) string {
	head := s.panelHead.Render("Status")
	transport := s.bad.Render("down")
	if m.status.transport {
		transport = s.good.Render("up")
	}
	key := s.bad.Render("locked")
	if m.status.unlocked {
		key = s.good.Render("unlocked")
	}
	listen := m.status.listen
	if listen == "" {
		listen = s.dim.Render("(none)")
	} else {
		// Truncate the listen address so it fits one line.
		contentW := w - 2 - 2
		listenW := contentW - len("listen    ")
		if listenW < 8 {
			listenW = 8
		}
		listen = s.accent.Render(truncate(listen, listenW))
	}
	lines := []string{
		fmt.Sprintf("peers     %s", s.accent.Render(fmt.Sprintf("%d", m.status.peers))),
		fmt.Sprintf("transport %s", transport),
		fmt.Sprintf("key       %s", key),
		fmt.Sprintf("listen    %s", listen),
	}
	body := strings.Join(lines, "\n")
	return s.panel.Width(w).MaxHeight(maxH).Render(head + "\n" + body)
}

func renderCompose(m model, s styles, w int) string {
	label := s.composeLabel.Render(" compose ")
	if m.focus == focusCompose {
		label = s.composeFocus.Render(" compose ")
	}
	body := m.composeBuf.String()
	if m.focus == focusCompose {
		body += s.cursor.Render("▏")
	} else if body == "" {
		body = s.composeDim.Render("(tab to compose)")
	}
	return s.compose.Width(w).Render(label + " " + body)
}

func renderLog(m model, s styles, w, maxH int) string {
	head := s.logHead.Render("log")
	content := ""
	if len(m.logLines) == 0 {
		content = s.dim.Render("(no logs)")
	} else {
		content = strings.Join(m.logLines, "\n")
	}
	return s.logPanel.Width(w).MaxHeight(maxH).Render(head + "\n" + content)
}

// helpEntry is one row of the help overlay.
type helpEntry struct {
	key  string
	desc string
}

var helpEntries = []helpEntry{
	{"tab", "focus compose / back to feed"},
	{"enter", "post the composed text"},
	{"esc", "cancel compose, return to feed"},
	{"j / k", "move feed selection down / up"},
	{"g / G", "jump to top / bottom of feed"},
	{"r", "refresh now"},
	{"h / ?", "toggle this help"},
	{"q", "quit"},
}

// renderHelp overlays a centered help panel on top of the normal view.
func renderHelp(s styles, w, h int, _ string) string {
	rows := make([]string, 0, len(helpEntries)+1)
	rows = append(rows, s.helpTitle.Render("Help"))
	for _, e := range helpEntries {
		rows = append(rows, fmt.Sprintf("  %s   %s",
			s.helpKey.Render(padRight(e.key, 7)),
			s.helpDesc.Render(e.desc)))
	}
	body := strings.Join(rows, "\n")
	innerW := 42
	innerH := len(helpEntries) + 4
	// Place the help box roughly in the center of the screen.
	x := (w - innerW) / 2
	if x < 0 {
		x = 0
	}
	box := s.helpBorder.Width(innerW).Height(innerH).Render(body)
	// Pad above so the box sits vertically centered.
	padTop := (h - innerH) / 2
	if padTop < 0 {
		padTop = 0
	}
	padded := strings.Repeat("\n", padTop) + box
	return lipgloss.Place(w, h, lipgloss.Position(float64(x)/float64(max(w,1))), lipgloss.Top, padded)
}

// Run starts the TUI as a client of the daemon control socket. The daemon
// must be running and unlocked; Run does not prompt for a passphrase.
func Run(socket, identity string) error {
	m := newModel(socket, identity)
	p := tea.NewProgram(m)
	_, err := p.Run()
	return err
}

// quitCmd is the tea.Cmd that signals Bubble Tea to exit.
func quitCmd() tea.Msg {
	return tea.Quit()
}

// shortID is a passthrough kept for call-site readability; the TUI shows
// full identifiers since the layout has room for them.
func shortID(id string) string {
	return id
}

// ageOf renders the age of a post. The timestamp is unix nanoseconds, the
// unit used throughout the event log (see core.Now64).
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
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}

// truncate caps s to n visible characters, appending an ellipsis if it was
// longer. Used to keep single-line fields from wrapping inside fixed-width
// panels.
func truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	if n == 1 {
		return "…"
	}
	return s[:n-1] + "…"
}

// Compile-time check: keep json import for future RPC param extension.
var _ = json.Marshal
