package tui

import (
	"strings"
	"testing"

	"charm.land/bubbles/v2/list"
	tea "charm.land/bubbletea/v2"
)

// asModel asserts the tea.Model returned by Update back to the concrete model.
func asModel(t *testing.T, m tea.Model) model {
	t.Helper()
	mm, ok := m.(model)
	if !ok {
		t.Fatalf("want model, got %T", m)
	}
	return mm
}

// press sends a key and returns the resulting concrete model, dropping the cmd.
func press(t *testing.T, m model, k tea.KeyPressMsg) model {
	t.Helper()
	mm, _ := m.Update(k)
	return asModel(t, mm)
}

func TestNewModel(t *testing.T) {
	m := newModel("", "driftnode:test")
	if m.activeTab != tabFeed {
		t.Fatalf("activeTab: want %d, got %d", tabFeed, m.activeTab)
	}
	if cmd := m.Init(); cmd == nil {
		t.Fatal("Init should return a command")
	}
}

func TestCtrlCQuits(t *testing.T) {
	m := newModel("", "driftnode:test")
	_, cmd := m.Update(tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl, Text: ""})
	if cmd == nil {
		t.Fatal("ctrl+c should quit")
	}
}

func TestQuitWithTextFallsThrough(t *testing.T) {
	m := newModel("", "driftnode:test")
	// 'q' is a regular character now: it lands in compose, not quit.
	m = press(t, m, tea.KeyPressMsg{Code: 'q', Text: "q"})
	if m.compose.value() != "q" {
		t.Fatalf("compose: want %q, got %q", "q", m.compose.value())
	}
}

func TestEscQuitsWhenEmpty(t *testing.T) {
	m := newModel("", "driftnode:test")
	mm, cmd := m.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	if cmd == nil {
		t.Fatal("esc with empty compose should quit")
	}
	_ = mm
}

func TestTabSwitching(t *testing.T) {
	m := newModel("", "driftnode:test")
	m = press(t, m, tea.KeyPressMsg{Code: tea.KeyTab})
	if m.activeTab != tabZens {
		t.Fatalf("tab: want %d, got %d", tabZens, m.activeTab)
	}
	// shift+tab cycles back to Feed.
	m = press(t, m, tea.KeyPressMsg{Code: tea.KeyTab, Mod: tea.ModShift})
	if m.activeTab != tabFeed {
		t.Fatalf("shift+tab: want %d, got %d", tabFeed, m.activeTab)
	}
}

func TestComposeAndPost(t *testing.T) {
	m := newModel("", "driftnode:test")
	// Type "hi".
	m = press(t, m, tea.KeyPressMsg{Code: 'h', Text: "h"})
	m = press(t, m, tea.KeyPressMsg{Code: 'i', Text: "i"})
	if m.compose.value() != "hi" {
		t.Fatalf("compose: want %q, got %q", "hi", m.compose.value())
	}
	// Enter triggers a post; compose resets.
	mm, cmd := m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	if cmd == nil {
		t.Fatal("enter should produce a post command")
	}
	if asModel(t, mm).compose.value() != "" {
		t.Fatal("compose should clear after enter")
	}
}

func TestEscClearsCompose(t *testing.T) {
	m := newModel("", "driftnode:test")
	m = press(t, m, tea.KeyPressMsg{Code: 'h', Text: "h"})
	m = press(t, m, tea.KeyPressMsg{Code: tea.KeyEsc})
	if m.compose.value() != "" {
		t.Fatal("esc should clear the compose line")
	}
}

func TestViewRendersIdentity(t *testing.T) {
	m := newModel("", "driftnode:test")
	m.width, m.height = 80, 24
	m.layout()
	out := m.View().Content
	if !strings.Contains(out, "driftnode:test") {
		t.Fatal("view should contain the identity")
	}
	// The active tab label should appear.
	if !strings.Contains(out, "Feed") {
		t.Fatal("view should render the active tab")
	}
}

func TestViewRendersFeed(t *testing.T) {
	m := newModel("", "driftnode:test")
	m.width, m.height = 80, 24
	m.layout()
	m.posts = []feedPost{{name: "alice", author: "driftnode:alice", text: "gm", age: "1m"}}
	m.feed.setPosts(m.posts, m.styles, 80)
	out := m.View().Content
	if !strings.Contains(out, "gm") {
		t.Fatal("view should render feed text")
	}
}

func TestStatusLine(t *testing.T) {
	m := newModel("", "driftnode:test")
	m.width, m.height = 80, 24
	m.layout()
	m.status = statusInfo{zens: 3, transport: true, unlocked: false}
	out := m.View().Content
	if !strings.Contains(out, "locked") {
		t.Fatal("status should render key state")
	}
	if !strings.Contains(out, "up") {
		t.Fatal("status should render transport up")
	}
}

func TestZenMenu(t *testing.T) {
	m := newModel("", "driftnode:test")
	m.width, m.height = 80, 24
	m.layout()
	// Seed the zens list with one discovered zen.
	m.zenList = []zen{{name: "alice", identity: "driftnode:alice", status: "connected"}}
	items := make([]list.Item, 0, 1)
	for _, z := range m.zenList {
		items = append(items, zenItem{z})
	}
	_ = m.zens.SetItems(items)
	// Switch to Zens tab and open the menu with enter.
	m = press(t, m, tea.KeyPressMsg{Code: tea.KeyTab})
	if m.activeTab != tabZens {
		t.Fatal("should be on Zens tab")
	}
	m = press(t, m, tea.KeyPressMsg{Code: tea.KeyEnter})
	if m.mode != modeMenu {
		t.Fatal("enter on a zen should open the action menu")
	}
	// The menu should offer info, and follow (alice is not yet followed).
	if len(m.menu) < 2 {
		t.Fatalf("menu should have at least 2 entries, got %d", len(m.menu))
	}
	// Press 1 to run "info": closes the menu and sets a notice.
	m = press(t, m, tea.KeyPressMsg{Code: '1', Text: "1"})
	if m.mode != modeBrowse {
		t.Fatal("info should close the menu")
	}
	if !strings.Contains(m.notice, "alice") {
		t.Fatalf("info notice should mention alice, got %q", m.notice)
	}
}
