package tui

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
)

func TestModelInit(t *testing.T) {
	m := newModel("", "driftnode:test")
	if cmd := m.Init(); cmd == nil {
		t.Fatal("Init should return a refresh command")
	}
}

func TestModelQuit(t *testing.T) {
	m := newModel("", "driftnode:test")
	// q quits.
	m2, cmd := m.Update(tea.KeyPressMsg{Code: 'q', Text: "q"})
	if cmd == nil {
		t.Fatal("q should produce a quit command")
	}
	_ = m2
}

func TestModelComposeAndPost(t *testing.T) {
	m := newModel("", "driftnode:test")
	// Tab focuses the compose widget.
	m2, _ := m.Update(tea.KeyPressMsg{Code: tea.KeyTab})
	m = m2.(model)
	if m.focus != focusCompose {
		t.Fatal("tab should focus compose")
	}
	// Type some text.
	m2, _ = m.Update(tea.KeyPressMsg{Code: 'h', Text: "h"})
	m = m2.(model)
	m2, _ = m.Update(tea.KeyPressMsg{Code: 'e', Text: "e"})
	m = m2.(model)
	m2, _ = m.Update(tea.KeyPressMsg{Code: 'l', Text: "l"})
	m = m2.(model)
	m2, _ = m.Update(tea.KeyPressMsg{Code: 'l', Text: "l"})
	m = m2.(model)
	m2, _ = m.Update(tea.KeyPressMsg{Code: 'o', Text: "o"})
	m = m2.(model)
	if m.composeBuf.String() != "hello" {
		t.Fatalf("compose buf: want %q, got %q", "hello", m.composeBuf.String())
	}
	// Enter triggers a post command; compose exits, buffer clears.
	m2, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	m = m2.(model)
	if m.focus != focusFeed {
		t.Fatal("enter should return focus to feed")
	}
	if m.composeBuf.Len() != 0 {
		t.Fatal("compose buffer should be cleared after enter")
	}
}

func TestModelEscExitsCompose(t *testing.T) {
	m := newModel("", "driftnode:test")
	m2, _ := m.Update(tea.KeyPressMsg{Code: tea.KeyTab})
	m = m2.(model)
	if m.focus != focusCompose {
		t.Fatal("tab should focus compose")
	}
	m2, _ = m.Update(tea.KeyPressMsg{Code: 'h', Text: "h"})
	m = m2.(model)
	if m.composeBuf.String() != "h" {
		t.Fatalf("compose buf: want %q, got %q", "h", m.composeBuf.String())
	}
	m2, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyEsc})
	m = m2.(model)
	if m.focus != focusFeed {
		t.Fatal("esc should return focus to feed")
	}
	if m.composeBuf.Len() != 0 {
		t.Fatal("compose buffer should be cleared on esc")
	}
}

func TestModelHelp(t *testing.T) {
	m := newModel("", "driftnode:test")
	m2, _ := m.Update(tea.KeyPressMsg{Code: 'h', Text: "h"})
	m = m2.(model)
	if !m.help {
		t.Fatal("h should show help")
	}
	// Any key dismisses help.
	m2, _ = m.Update(tea.KeyPressMsg{Code: 'q', Text: "q"})
	m = m2.(model)
	if m.help {
		t.Fatal("any key should dismiss help")
	}
}

func TestModelViewRenders(t *testing.T) {
	m := newModel("", "driftnode:test")
	m.width = 132
	m.height = 40
	m.feed = []feedItem{
		{author: "alice", text: "gm", age: "1m"},
	}
	out := m.View().Content
	if !strings.Contains(out, "driftnode:test") {
		t.Fatal("view should contain identity")
	}
	if !strings.Contains(out, "gm") {
		t.Fatal("view should contain feed text")
	}
	if !strings.Contains(out, "locked") {
		t.Fatal("view should render key status")
	}
	// The help hint key should not leak as a header hint line.
	if strings.Contains(out, "[q] quit") {
		t.Fatal("header should not contain inline keybinding hint")
	}
}
