package tui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func TestModelInit(t *testing.T) {
	m := newModel("", "driftnode:test")
	if cmd := m.Init(); cmd == nil {
		t.Fatal("Init should return a refresh command")
	}
}

func TestModelQuit(t *testing.T) {
	m := newModel("", "driftnode:test")
	// Esc unfocuses compose but doesn't quit; q quits.
	m2, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	if cmd == nil {
		t.Fatal("q should produce a quit command")
	}
	_ = m2
}

func TestModelComposeAndPost(t *testing.T) {
	m := newModel("", "driftnode:test")
	// Enter compose mode.
	m2, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'i'}})
	m = m2.(model)
	if !m.composing {
		t.Fatal("should be composing after 'i'")
	}
	// Type some text.
	m2, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("hello")})
	m = m2.(model)
	if m.composeBuf.String() != "hello" {
		t.Fatalf("compose buf: want %q, got %q", "hello", m.composeBuf.String())
	}
	// Enter triggers a post command; compose mode exits, buffer clears.
	m2, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = m2.(model)
	if m.composing {
		t.Fatal("should exit compose mode after enter")
	}
	if m.composeBuf.Len() != 0 {
		t.Fatal("compose buffer should be cleared after enter")
	}
}

func TestModelEscExitsCompose(t *testing.T) {
	m := newModel("", "driftnode:test")
	m2, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'i'}})
	m = m2.(model)
	if !m.composing {
		t.Fatal("should be composing")
	}
	m2, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = m2.(model)
	if m.composing {
		t.Fatal("esc should exit compose mode")
	}
	if m.composeBuf.Len() != 0 {
		t.Fatal("compose buffer should be cleared on esc")
	}
}

func TestModelViewRenders(t *testing.T) {
	m := newModel("", "driftnode:test")
	m.width = 80
	m.height = 24
	m.feed = []feedItem{
		{author: "alice", text: "gm", age: "1m"},
	}
	out := m.View()
	if !strings.Contains(out, "driftnode:test") {
		t.Fatal("view should contain identity")
	}
	if !strings.Contains(out, "gm") {
		t.Fatal("view should contain feed text")
	}
	if !strings.Contains(out, "key: locked") {
		t.Fatal("view should render key status")
	}
}
