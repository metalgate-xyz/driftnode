package tui

import (
	"strings"
	"testing"

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

func TestZenActions(t *testing.T) {
	m := newModel("", "driftnode:test")
	m.width, m.height = 80, 24
	m.layout()
	// Seed the zens list with one discovered zen.
	m.zenList = []zen{{name: "alice", identity: "driftnode:alice", status: "connected"}}
	m.zens.setItems(m.zenList)
	// Switch to Zens tab.
	m = press(t, m, tea.KeyPressMsg{Code: tea.KeyTab})
	if m.activeTab != tabZens {
		t.Fatal("should be on Zens tab")
	}
	// 'i' shows info as a notice, without opening any menu.
	m = press(t, m, tea.KeyPressMsg{Code: 'i', Text: "i"})
	if !strings.Contains(m.notice, "alice") {
		t.Fatalf("info notice should mention alice, got %q", m.notice)
	}
	// 'f' on an unfollowed zen issues a follow command.
	mm, cmd := m.Update(tea.KeyPressMsg{Code: 'f', Text: "f"})
	if cmd == nil {
		t.Fatal("follow should produce a command")
	}
	m = asModel(t, mm)
	// 'f' again on an already-followed zen does not issue a command.
	m.followList = []identityEntry{{identity: "driftnode:alice", name: "alice"}}
	m.follows.setItems(m.followList)
	mm, cmd = m.Update(tea.KeyPressMsg{Code: 'f', Text: "f"})
	if cmd != nil {
		t.Fatal("follow on an already-followed zen should not produce a command")
	}
	if !strings.Contains(asModel(t, mm).notice, "already following") {
		t.Fatal("should report already following")
	}
}

// TestMouseClickSelectsZen verifies a left-click on a list row selects the item
// at that row. The coordinate math accounts for the three screen rows above
// the panel content: title, tab bar, and the panel's top border.
func TestMouseClickSelectsZen(t *testing.T) {
	m := simModel(t, 80, 24, "driftnode:test", 5)
	for i := range m.zenList {
		m.zenList[i] = zen{name: "zen" + string(rune('a'+i)), identity: "driftnode:z" + string(rune('a'+i)), status: "connected"}
	}
	m.zens.setItems(m.zenList)
	m.activeTab = tabZens

	// Each zen row is 2 lines (title + description). Click the third item:
	// content row 4-5 maps to screen row 4+3=7 (its title line).
	mm, _ := m.Update(tea.MouseClickMsg{X: 5, Y: 7, Button: tea.MouseLeft})
	m = asModel(t, mm)
	z, ok := m.zens.selectedItem()
	if !ok || z.name != "zenc" {
		t.Fatalf("click on row 4 should select zenc, got %+v ok=%v", z, ok)
	}
}

// TestMouseClickIgnoresNonLeftButton ensures only left clicks select.
func TestMouseClickIgnoresNonLeftButton(t *testing.T) {
	m := simModel(t, 80, 24, "driftnode:test", 3)
	for i := range m.zenList {
		m.zenList[i] = zen{name: "zen" + string(rune('a'+i)), identity: "driftnode:z" + string(rune('a'+i)), status: "connected"}
	}
	m.zens.setItems(m.zenList)
	m.activeTab = tabZens

	mm, _ := m.Update(tea.MouseClickMsg{X: 5, Y: 3, Button: tea.MouseRight})
	m = asModel(t, mm)
	z, _ := m.zens.selectedItem()
	if z.name != "zena" {
		t.Fatalf("right click should not change selection, got %q", z.name)
	}
}

// TestMouseClickOutOfBoundsIgnored ensures clicks below the last item don't
// wrap or select garbage.
func TestMouseClickOutOfBoundsIgnored(t *testing.T) {
	m := simModel(t, 80, 24, "driftnode:test", 2)
	for i := range m.zenList {
		m.zenList[i] = zen{name: "zen" + string(rune('a'+i)), identity: "driftnode:z" + string(rune('a'+i)), status: "connected"}
	}
	m.zens.setItems(m.zenList)
	m.activeTab = tabZens

	// Click far below the last item (screen row 30).
	mm, _ := m.Update(tea.MouseClickMsg{X: 5, Y: 30, Button: tea.MouseLeft})
	m = asModel(t, mm)
	z, ok := m.zens.selectedItem()
	if !ok || z.name != "zena" {
		t.Fatalf("out-of-bounds click should leave selection at first item, got %q ok=%v", z.name, ok)
	}
}
