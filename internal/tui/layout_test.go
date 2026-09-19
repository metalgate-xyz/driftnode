package tui

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
)

// simModel drives the model through the same messages the real program sees:
// a WindowSizeMsg to set the geometry, then a refreshMsg to populate the lists.
func simModel(t *testing.T, w, h int, identity string, zenCount int) model {
	t.Helper()
	m := newModel("", identity)
	mm, _ := m.Update(tea.WindowSizeMsg{Width: w, Height: h})
	m = mm.(model)
	rm := refreshMsg{
		zens: make([]zen, zenCount),
	}
	for i := range rm.zens {
		rm.zens[i] = zen{name: "alice", identity: identity, status: "connected", verified: true}
	}
	m.applyRefresh(rm)
	m.status = statusInfo{zens: zenCount, transport: true, unlocked: true}
	return m
}

func TestStatusLineAlwaysVisible(t *testing.T) {
	identities := []string{
		"driftnode:test",
		"driftnode:abcdefghijklmnopqrstuvwxyz0123456789abcdefghij",
	}
	var failures int
	for _, id := range identities {
		for _, h := range []int{8, 10, 12, 16, 20, 24, 30, 40, 50} {
			for _, w := range []int{40, 60, 80, 100, 120} {
				for _, n := range []int{0, 1, 3, 7, 20, 100} {
					m := simModel(t, w, h, id, n)
					// Check every tab.
					for tab := 0; tab < len(tabs); tab++ {
						m.activeTab = tab
						out := m.View().Content
						lines := strings.Split(out, "\n")
						missing := !strings.Contains(out, "transport")
						overflow := len(lines) > h
						if missing || overflow {
							failures++
							t.Errorf("tab=%d id_len=%d w=%d h=%d n=%d: lines=%d statusMissing=%v overflow=%v",
								tab, len(id), w, h, n, len(lines), missing, overflow)
						}
					}
				}
			}
		}
	}
	if failures > 0 {
		t.Fatalf("%d failures", failures)
	}
}

func TestZensListItemHeight(t *testing.T) {
	m := simModel(t, 80, 24, "driftnode:abcdefghijklmnopqrstuvwxyz0123456789abcdefghij", 7)
	m.activeTab = tabZens
	out := m.View().Content
	lines := strings.Split(out, "\n")
	t.Logf("height=24 rendered=%d", len(lines))
	for i, l := range lines {
		t.Logf("  %2d: %q", i, l)
	}
}
