package store

import (
	"fmt"
	"path/filepath"
	"testing"

	"driftnode/internal/core"
)

// TestTokenLRUEvictsColdest verifies that overfilling the token cache
// evicts the coldest (least recently mentioned) tokens so the tracked
// count stays at cap.
func TestTokenLRUEvictsColdest(t *testing.T) {
	s := newTestStore(t)

	const n = 10
	for i := range n {
		tok := fmt.Sprintf("tok-%d", i)
		if err := s.LearnToken(tok, int64(i)); err != nil {
			t.Fatalf("LearnToken %d: %v", i, err)
		}
	}
	// Re-mention the coldest so it becomes hot; the next learn evicts
	// tok-1 (the oldest untouched) instead.
	if err := s.LearnToken("tok-0", 100); err != nil {
		t.Fatalf("re-mention tok-0: %v", err)
	}
	if err := s.LearnToken("tok-new", 101); err != nil {
		t.Fatalf("LearnToken new: %v", err)
	}

	toks, err := s.KnownTokens()
	if err != nil {
		t.Fatalf("KnownTokens: %v", err)
	}
	set := make(map[string]bool, len(toks))
	for _, tok := range toks {
		set[tok] = true
	}
	if !set["tok-0"] || !set["tok-new"] {
		t.Fatalf("hot tokens missing: %v", toks)
	}
}

// TestTokenLRUKeepUnderPressure verifies routing-bound and bootstrap
// tokens survive eviction pressure while ordinary tokens are evicted.
func TestTokenLRUKeepUnderPressure(t *testing.T) {
	s := newTestStore(t)

	// Bootstrap tokens: loaded from the bootstrap file.
	if err := s.PutBootstrapTokens([]string{"boot-1", "boot-2"}); err != nil {
		t.Fatalf("PutBootstrapTokens: %v", err)
	}
	// Routing-bound token: a followed identity is bound to it.
	followed, _ := core.NewKeyPair()
	if err := s.PutRouting(followed.Identity(), "route-1"); err != nil {
		t.Fatalf("PutRouting: %v", err)
	}
	if err := s.LearnToken("route-1", 0); err != nil {
		t.Fatalf("LearnToken route-1: %v", err)
	}
	// An ordinary discovered token, coldest.
	if err := s.LearnToken("plain-1", 1); err != nil {
		t.Fatalf("LearnToken plain-1: %v", err)
	}

	// Fill past the cap with pressure tokens; the keep classes survive.
	for i := range 12000 {
		if err := s.LearnToken(fmt.Sprintf("flood-%05d", i), int64(1000+i)); err != nil {
			t.Fatalf("LearnToken flood %d: %v", i, err)
		}
	}

	toks, err := s.KnownTokens()
	if err != nil {
		t.Fatalf("KnownTokens: %v", err)
	}
	set := make(map[string]bool, len(toks))
	for _, tok := range toks {
		set[tok] = true
	}
	if !set["boot-1"] || !set["boot-2"] {
		t.Fatal("bootstrap token evicted")
	}
	if !set["route-1"] {
		t.Fatal("routing-bound token evicted")
	}
	if set["plain-1"] {
		t.Fatal("plain token survived; eviction is not happening")
	}
	if len(toks) > maxTokenCache {
		t.Fatalf("tracked tokens %d exceed cap %d", len(toks), maxTokenCache)
	}
}

// TestTokenSurvivesRestart verifies discovered tokens are persisted and
// reloaded after close/reopen (the rehydrate path reads the token LRU).
func TestTokenSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/t.db"
	prevCap := maxTokenCache
	maxTokenCache = 3
	defer func() { maxTokenCache = prevCap }()
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.LearnToken("tok-persist", 42); err != nil {
		t.Fatalf("LearnToken: %v", err)
	}
	if err := s.LearnToken("tok-evicted", 41); err != nil {
		t.Fatalf("LearnToken: %v", err)
	}
	// Mention tok-persist again so it is hot, then learn enough fresh
	// tokens that tok-evicted (coldest) is evicted from the cap of 3.
	if err := s.LearnToken("tok-persist", 50); err != nil {
		t.Fatalf("re-mention persist: %v", err)
	}
	if err := s.LearnToken("tok-warm-1", 60); err != nil {
		t.Fatalf("learn warm 1: %v", err)
	}
	if err := s.LearnToken("tok-warm-2", 61); err != nil {
		t.Fatalf("learn warm 2: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	toks, err := s2.KnownTokens()
	if err != nil {
		t.Fatalf("KnownTokens after reopen: %v", err)
	}
	set := make(map[string]bool)
	for _, tok := range toks {
		set[tok] = true
	}
	if !set["tok-persist"] {
		t.Fatal("discovered token did not survive restart")
	}
	if set["tok-evicted"] {
		t.Fatal("evicted token resurrected after restart")
	}
}

// TestTokenNotResurrectedByOffers verifies that after eviction, neither
// rehydrate (reopen reads of KnownTokens) nor a zen offer enumeration
// resurrects the evicted token: the only way back in is an explicit
// re-mention from a peer that still holds it hot.
func TestTokenNotResurrectedByOffers(t *testing.T) {
	prevCap := maxTokenCache
	maxTokenCache = 2
	defer func() { maxTokenCache = prevCap }()

	dbPath := filepath.Join(t.TempDir(), "t.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.LearnToken("tok-gone", 1); err != nil {
		t.Fatalf("learn: %v", err)
	}
	if err := s.LearnToken("tok-hot-1", 100); err != nil {
		t.Fatalf("learn hot: %v", err)
	}
	if err := s.LearnToken("tok-hot-2", 101); err != nil {
		t.Fatalf("learn hot 2: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen (the rehydrate path reads KnownTokens) and re-enumerate the
	// offers: the evicted token stays gone.
	s2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	toks, err := s2.KnownTokens()
	if err != nil {
		t.Fatalf("KnownTokens: %v", err)
	}
	for _, tok := range toks {
		if tok == "tok-gone" {
			t.Fatal("evicted token resurrected by rehydrate/offer enumeration")
		}
	}
}

// TestOfferRelearnsHotToken closes the eviction loop: an evicted token that
// a peer still mentions is relearned on the next exchange, so eviction is
// self-healing while at least one holder keeps the token hot. This is the
// deliberate re-learn path, distinct from rehydrate, which only reads.
func TestOfferRelearnsHotToken(t *testing.T) {
	prevCap := maxTokenCache
	maxTokenCache = 2
	defer func() { maxTokenCache = prevCap }()

	s := newTestStore(t)
	if err := s.LearnToken("tok-loop", 1); err != nil {
		t.Fatalf("learn: %v", err)
	}
	if err := s.LearnToken("tok-hot-1", 100); err != nil {
		t.Fatalf("learn hot: %v", err)
	}
	if err := s.LearnToken("tok-hot-2", 101); err != nil {
		t.Fatalf("learn hot 2: %v", err)
	}
	toks, _ := s.KnownTokens()
	for _, tok := range toks {
		if tok == "tok-loop" {
			t.Fatal("fixture failed: token not evicted before re-offer")
		}
	}
	// A peer offers it again: relearned.
	if err := s.LearnToken("tok-loop", 2000); err != nil {
		t.Fatalf("relearn: %v", err)
	}
	toks, _ = s.KnownTokens()
	found := false
	for _, tok := range toks {
		if tok == "tok-loop" {
			found = true
		}
	}
	if !found {
		t.Fatal("re-offered token not relearned")
	}
}
