package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type model struct {
	width      int
	height     int
	identity   string
	feed       []feedItem
	peers      []peerInfo
	status     statusInfo
	logLines   []string
	cursor     int
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
}

func newModel(identity string) model {
	return model{
		identity: identity,
		status:   statusInfo{bootstrap: "not verified"},
	}
}

func (m model) Init() tea.Cmd { return nil }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
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
			m.composing = true
		case "esc":
			m.composing = false
			m.composeBuf.Reset()
		case "enter":
			if m.composing {
				text := m.composeBuf.String()
				if text != "" {
					m.feed = append([]feedItem{{author: "you", text: text, age: "now"}}, m.feed...)
				}
				m.composing = false
				m.composeBuf.Reset()
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
		"sync round: #" + fmt.Sprintf("%d", m.status.syncRound),
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

// Run starts the TUI.
func Run(identity string) error {
	m := newModel(identity)
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
