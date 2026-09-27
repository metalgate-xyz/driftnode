package tui

import (
	tea "charm.land/bubbletea/v2"
)

// model is the Bubble Tea model. It holds the daemon socket, the active tab,
// the panels, and the live subscription state.
type model struct {
	socket   string
	identity string
	width    int
	height   int
	styles   styles

	activeTab int
	notice    string

	feed      feedPanel
	zens      selectList[zen]
	follows   selectList[zen]
	followers selectList[zen]
	compose   compose

	// sub is the open subscribe connection, or nil between reconnects. The
	// daemon pushes diffs over it; the TUI applies them in place so the
	// panels are never rebuilt from scratch.
	sub *subscription

	status statusInfo
}

func newModel(socket, identity string) model {
	m := model{
		socket:    socket,
		identity:  identity,
		styles:    newStyles(),
		activeTab: tabFeed,
	}
	m.compose = newCompose()
	// The compose input is always focused: typing lands in the post box
	// without a separate focus step. We focus here so tests (which call
	// Update directly without running Init's command) also capture input.
	m.compose.focus()
	// Lists start at size 0; layout() sizes them once the window is known.
	m.zens = newZenList(0, 0)
	m.follows = newFollowList(0, 0)
	m.followers = newFollowList(0, 0)
	return m
}

// Init opens the subscription. The compose input is always focused so typing
// lands in the post box without a separate focus step.
func (m model) Init() tea.Cmd {
	return tea.Batch(m.compose.focus(), subscribeCmd(m.socket))
}

// Update is the Elm Architecture entry point.
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.layout()
		return m, nil
	case snapshotMsg:
		if msg.err != nil {
			m.notice = "daemon: " + msg.err.Error()
			return m, reconnectCmd(m.socket)
		}
		m.applySnapshot(msg)
		m.sub = msg.sub
		return m, subscribeContinueCmd(m.sub)
	case diffMsg:
		m.applyDiff(msg)
		return m, subscribeContinueCmd(msg.sub)
	case subClosedMsg:
		m.sub = nil
		m.notice = "reconnecting…"
		return m, reconnectCmd(m.socket)
	case reconnectTickMsg:
		return m, subscribeCmd(m.socket)
	case actionResultMsg:
		if msg.err != nil {
			m.notice = msg.err.Error()
		} else {
			m.notice = msg.notice
		}
		return m, nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	case tea.MouseWheelMsg:
		return m.routeMouse(msg)
	case tea.MouseClickMsg:
		return m.routeClick(msg)
	}
	return m, nil
}

// applySnapshot seeds all panels from the initial subscribe snapshot.
func (m *model) applySnapshot(msg snapshotMsg) {
	m.feed.setPosts(msg.feed)
	m.zens.setItems(msg.zens)
	m.follows.setItems(msg.follows)
	m.followers.setItems(msg.followers)
	m.status = msg.status
	m.notice = ""
}

// applyDiff applies one incremental push event to the matching panel. Each
// panel upserts/removes by key, so only the changed row moves and the scroll
// position and selection survive.
func (m *model) applyDiff(d diffMsg) {
	switch d.panel {
	case "feed":
		for _, p := range d.add {
			m.feed.upsert(p)
		}
	case "zens":
		if d.upsert != nil {
			m.zens.upsert(*d.upsert)
		}
		if d.removeID != "" {
			m.zens.remove(d.removeID)
		}
	case "follows":
		for _, z := range d.addZens {
			m.follows.upsert(z)
		}
		if d.removeID != "" {
			m.follows.remove(d.removeID)
		}
	case "followers":
		for _, z := range d.addZens {
			m.followers.upsert(z)
		}
		if d.removeID != "" {
			m.followers.remove(d.removeID)
		}
	case "status":
		if d.status != nil {
			m.status = *d.status
		}
	}
}

// layout sizes all panels for the current window. Called on WindowSizeMsg.
func (m *model) layout() {
	if m.width == 0 || m.height == 0 {
		return
	}
	w, h := panelContentWidth(panelWidth(m.width)), panelContentHeight(panelHeight(m.height))
	m.feed.resize(w, h)
	m.zens.resize(w, h)
	m.follows.resize(w, h)
	m.followers.resize(w, h)
	// Keep the compose input width sensible: leave room for the prompt.
	m.compose.input.SetWidth(m.width - 4)
}

// routeMouse forwards wheel events to the active panel.
func (m model) routeMouse(msg tea.MouseWheelMsg) (tea.Model, tea.Cmd) {
	switch m.activeTab {
	case tabFeed:
		var cmd tea.Cmd
		m.feed, cmd = m.feed.update(msg)
		return m, cmd
	case tabZens:
		var cmd tea.Cmd
		m.zens, cmd = m.zens.update(msg)
		return m, cmd
	case tabFollows:
		var cmd tea.Cmd
		m.follows, cmd = m.follows.update(msg)
		return m, cmd
	case tabFollowers:
		var cmd tea.Cmd
		m.followers, cmd = m.followers.update(msg)
		return m, cmd
	}
	return m, nil
}

// routeClick maps a left-click to a list row on the selectable tabs. Only
// left clicks select. A click on the tab bar switches tabs; the feed is also
// selectable: a click in its panel picks the post.
func (m model) routeClick(msg tea.MouseClickMsg) (tea.Model, tea.Cmd) {
	if msg.Button != tea.MouseLeft {
		return m, nil
	}
	// The tab bar is the row below the title.
	if msg.Y == 1 {
		if i := m.tabAt(msg.X); i >= 0 {
			m.activeTab = i
		}
		return m, nil
	}
	// Screen rows above the panel content: title (1) + tab bar (1) + panel
	// top border (1).
	const panelContentTop = 3
	row := msg.Y - panelContentTop
	switch m.activeTab {
	case tabFeed:
		m.feed.click(row)
		return m, nil
	case tabZens:
		m.zens.click(row)
		return m, nil
	case tabFollows:
		m.follows.click(row)
		return m, nil
	case tabFollowers:
		m.followers.click(row)
		return m, nil
	}
	return m, nil
}

// panelWidth and panelHeight reserve rows for the title, tab bar, compose
// line, and status line, returning the outer size allotted to the panel.
func panelWidth(w int) int { return w }

func panelHeight(h int) int {
	// title (1) + tab bar (1) + compose (1) + status (1).
	body := h - 4
	if body < 4 {
		body = 4
	}
	return body
}

// panelContentWidth and panelContentHeight subtract the panel's border and
// padding so the viewport/lists render at the inner box size. Without this
// the inner content is wider/taller than the panel frame and wraps, pushing
// the compose line and status bar off-screen.
func panelContentWidth(w int) int {
	return w - theme.panel.GetHorizontalFrameSize()
}

func panelContentHeight(h int) int {
	return h - theme.panel.GetVerticalFrameSize()
}
