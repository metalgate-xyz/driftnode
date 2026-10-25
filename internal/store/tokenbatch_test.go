package store

import (
	"fmt"
	"testing"
	"time"
)

// Learning a batch of tokens is one maintenance pass: every token is
// tracked, the coldest entries are evicted down to the cap, and the
// tracked count never exceeds it.
func TestLearnTokensBatch(t *testing.T) {
	s := newTestStore(t)

	prevCap := maxTokenCache
	maxTokenCache = 5
	defer func() { maxTokenCache = prevCap }()

	batch := make([]string, 20)
	for i := range batch {
		batch[i] = fmt.Sprintf("tok-%02d", i)
	}
	if err := s.LearnTokens(batch, 100); err != nil {
		t.Fatalf("LearnTokens: %v", err)
	}

	toks, err := s.KnownTokens()
	if err != nil {
		t.Fatalf("KnownTokens: %v", err)
	}
	if len(toks) != maxTokenCache {
		t.Fatalf("tracked tokens: want %d, got %d", maxTokenCache, len(toks))
	}
	// Same-batch ties break by id: the hottest (last-touched) survivors
	// are the lexicographically greatest batch members.
	set := make(map[string]bool, len(toks))
	for _, tok := range toks {
		set[tok] = true
	}
	if !set["tok-19"] || set["tok-00"] {
		t.Fatalf("batch eviction kept the wrong survivors: %v", toks)
	}

	// Mixing a re-mention into the next batch refreshes the old token.
	mixed := []string{"tok-19", "tok-new"}
	if err := s.LearnTokens(mixed, 200); err != nil {
		t.Fatalf("LearnTokens mixed: %v", err)
	}
	toks, err = s.KnownTokens()
	if err != nil {
		t.Fatalf("KnownTokens mixed: %v", err)
	}
	set = make(map[string]bool, len(toks))
	for _, tok := range toks {
		set[tok] = true
	}
	if !set["tok-19"] || !set["tok-new"] {
		t.Fatalf("re-mentioned token did not survive: %v", toks)
	}
}

// A batch learn forcing evictions for every token in a full 10k-cache
// completes well under a second: one maintenance pass, not one per token.
func TestLearnTokensBatchThroughput(t *testing.T) {
	s := newTestStore(t)

	n := maxTokenCache
	batch := make([]string, n)
	for i := range batch {
		batch[i] = fmt.Sprintf("tok-%08d", i)
	}
	start := time.Now()
	if err := s.LearnTokens(batch, 100); err != nil {
		t.Fatalf("LearnTokens: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("batch learn of %d tokens took %v", n, elapsed)
	}
	toks, err := s.KnownTokens()
	if err != nil {
		t.Fatalf("KnownTokens: %v", err)
	}
	if len(toks) != maxTokenCache {
		t.Fatalf("tracked tokens: want %d, got %d", maxTokenCache, len(toks))
	}
}
