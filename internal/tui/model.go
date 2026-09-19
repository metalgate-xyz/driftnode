package tui

import (
	"charm.land/bubbles/v2/list"
	tea "charm.land/bubbletea/v2"
)

// mode tracks whether we are browsing a tab or acting on a selected zen.
type mode int

const (
	modeBrowse mode = iota
	modeMenu
)

// menuItem is one operation offered for a selected zen.
type menuItem struct {
	label string
	run   func(model) (tea.Model, tea.Cmd)
}

// model is the Bubble Tea model. It holds the daemon socket, the active tab,
// the panels, and the shared refresh state.
type model struct {
	socket   string
	identity string
	width    int
	height   int
	styles   styles

	activeTab int
	mode      mode
	menu      []menuItem
	notice    string

	feed      feedPanel
	zens      list.Model
	follows   list.Model
	followers list.Model
	compose   compose

	posts        []feedPost
	zenList      []zen
	followList   []identityEntry
	followerList []identityEntry
	status       statusInfo
}

func newModel(socket, identity string) model {
	m := model{
		socket:    socket,
		identity:  identity,
		styles:    newStyles(),
		activeTab: tabFeed,
		mode:      modeBrowse,
	}
	m.compose = newCompose()
	// The compose input is always focused: typing lands in the post box
	// without a separate focus step. We focus here so tests (which call
	// Update directly without running Init's command) also capture input.
	m.compose.focus()
	// Lists start at size 0; layout() sizes them once the window is known.
	m.zens = newZenList(0, 0)
	m.follows = newFollowList("Follows", 0, 0)
	m.followers = newFollowList("Followers", 0, 0)
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
	}
	return m, nil
}

// applyRefresh merges a poll result into the model state.
func (m *model) applyRefresh(msg refreshMsg) {
	if msg.feedErr == nil {
		m.posts = msg.feed
		m.feed.setPosts(msg.feed, m.styles, panelWidth(m.width))
	} else {
		m.notice = "feed: " + msg.feedErr.Error()
	}
	if msg.zensErr == nil {
		m.zenList = msg.zens
		items := make([]list.Item, 0, len(msg.zens))
		for _, z := range msg.zens {
			items = append(items, zenItem{z})
		}
		_ = m.zens.SetItems(items)
	}
	if msg.followsErr == nil {
		m.followList = msg.follows
		setFollowItems(&m.follows, msg.follows)
	}
	if msg.followersErr == nil {
		m.followerList = msg.followers
		setFollowItems(&m.followers, msg.followers)
	}
	if msg.statusErr == nil {
		m.status = msg.status
	}
}

// refreshPanels re-applies sizes after content changes so the viewport and
// lists keep their geometry.
func (m model) refreshPanels() tea.Cmd {
	m.feed.resize(panelWidth(m.width), panelHeight(m.height))
	m.zens.SetSize(panelWidth(m.width), panelHeight(m.height))
	m.follows.SetSize(panelWidth(m.width), panelHeight(m.height))
	m.followers.SetSize(panelWidth(m.width), panelHeight(m.height))
	return nil
}

// layout sizes all panels for the current window. Called on WindowSizeMsg.
func (m *model) layout() {
	if m.width == 0 || m.height == 0 {
		return
	}
	w, h := panelWidth(m.width), panelHeight(m.height)
	if m.feed.vp.Width() == 0 {
		m.feed = newFeedPanel(w, h)
	} else {
		m.feed.resize(w, h)
		m.feed.setPosts(m.posts, m.styles, m.width)
	}
	m.zens.SetSize(w, h)
	m.follows.SetSize(w, h)
	m.followers.SetSize(w, h)
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
		m.zens, cmd = m.zens.Update(msg)
		return m, cmd
	case tabFollows:
		var cmd tea.Cmd
		m.follows, cmd = m.follows.Update(msg)
		return m, cmd
	case tabFollowers:
		var cmd tea.Cmd
		m.followers, cmd = m.followers.Update(msg)
		return m, cmd
	}
	return m, nil
}

// panelWidth and panelHeight reserve rows for the title, tab bar, compose
// line, and status line.
func panelWidth(w int) int { return w }

func panelHeight(h int) int {
	// title (1) + tab bar (1) + compose (1) + status (1).
	body := h - 4
	if body < 4 {
		body = 4
	}
	return body
}
