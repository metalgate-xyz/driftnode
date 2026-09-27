package tui

import (
	"bytes"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
)

// TestRunRendersSomething drives the real tea.Program (not just the model)
// with a canned window size, a ctrl+c to quit, and a buffer output, then
// checks the program wrote a frame before exiting. This catches wiring bugs
// a model-only test misses (Init command ordering, View before WindowSizeMsg,
// reconnect hot-loop, etc.).
func TestRunRendersSomething(t *testing.T) {
	m := newModel("/nonexistent/driftnode.sock", "driftnode:test")
	var out bytes.Buffer
	// ctrl+c (0x03) quits the program via handleKey.
	in := bytes.NewReader([]byte{0x03})
	p := tea.NewProgram(m,
		tea.WithWindowSize(80, 24),
		tea.WithInput(in),
		tea.WithOutput(&out),
		tea.WithoutSignalHandler(),
	)
	done := make(chan error, 1)
	go func() { _, err := p.Run(); done <- err }()
	select {
	case <-time.After(3 * time.Second):
		p.Quit()
		t.Fatal("program did not exit within 3s")
	case err := <-done:
		if err != nil {
			t.Logf("Run returned err: %v", err)
		}
	}
	body := out.String()
	t.Logf("OUTPUT (len=%d):\n%s", len(body), body)
	if strings.TrimSpace(body) == "" {
		t.Fatal("program produced no output")
	}
}

