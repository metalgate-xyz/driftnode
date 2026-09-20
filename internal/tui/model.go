package tui

import (
	tea "charm.land/bubbletea/v2"
)

// model is the Bubble Tea model. It holds the daemon socket, the active tab,
// the panels, and the shared refresh state.
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

	posts        []feedPost
	zenList      []zen
	followList   []zen
	followerList []zen
	status       statusInfo
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

// Init starts the first poll. The compose input is always focused so typing
// lands in the post box without a separate focus step.
func (m model) Init() tea.Cmd {
	return tea.Batch(m.compose.focus(), feedCmd(m.socket))
}

// Update is the Elm Architecture entry point.
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.layout()
		return m, nil
	case refreshMsg:
		m.applyRefresh(msg)
		return m, tea.Batch(tickCmd(m.socket), m.refreshPanels())
	case tickMsg:
		return m, feedCmd(m.socket)
	case actionResultMsg:
		if msg.err != nil {
			m.notice = msg.err.Error()
		} else {
			m.notice = msg.notice
		}
		return m, tea.Batch(tickCmd(m.socket), m.refreshPanels())
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	case tea.MouseWheelMsg:
		return m.routeMouse(msg)
	case tea.MouseClickMsg:
		return m.routeClick(msg)
	}
	return m, nil
}

// applyRefresh merges a poll result into the model state.
func (m *model) applyRefresh(msg refreshMsg) {
	if msg.feedErr == nil {
		m.posts = msg.feed
		m.feed.setPosts(msg.feed)
	} else {
		m.notice = "feed: " + msg.feedErr.Error()
	}
	if msg.zensErr == nil {
		m.zenList = msg.zens
		m.zens.setItems(msg.zens)
	}
	if msg.followsErr == nil {
		m.followList = msg.follows
		m.follows.setItems(msg.follows)
	}
	if msg.followersErr == nil {
		m.followerList = msg.followers
		m.followers.setItems(msg.followers)
	}
	if msg.statusErr == nil {
		m.status = msg.status
	}
}

// refreshPanels re-applies sizes after content changes so the viewport and
// lists keep their geometry.
func (m model) refreshPanels() tea.Cmd {
	w, h := panelContentWidth(panelWidth(m.width)), panelContentHeight(panelHeight(m.height))
	m.feed.resize(w, h)
	m.zens.resize(w, h)
	m.follows.resize(w, h)
	m.followers.resize(w, h)
	return nil
}

// layout sizes all panels for the current window. Called on WindowSizeMsg.
func (m *model) layout() {
	if m.width == 0 || m.height == 0 {
		return
	}
	w, h := panelContentWidth(panelWidth(m.width)), panelContentHeight(panelHeight(m.height))
	if m.feed.vp.Width() == 0 {
		m.feed = newFeedPanel(w, h)
	} else {
		m.feed.resize(w, h)
		m.feed.setPosts(m.posts)
	}
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
