package store

import (
	"testing"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// TestLRUSharedByCrawlAndToken verifies the LRU-reuse contract: the crawl
// cache and the token cache are specializations of the same generic
// boltLRU, so touch, count, and evict-to-cap behave identically in both,
// and only the keep predicate and the evict side effect differ: the crawl
// cache keeps followed identities and deletes the author bucket on
// eviction, the token cache keeps routing-bound and bootstrap tokens and
// has no eviction side effect.
func TestLRUSharedByCrawlAndToken(t *testing.T) {
	s := newTestStore(t)

	// Both caches are boltLRU values specialized over their bucket
	// pairs; the store's public methods must route through exactly
	// these instances.
	if crawlLRU.fwdBucket == nil || tokenLRU.fwdBucket == nil {
		t.Fatal("LRU specializations not configured")
	}
	if string(crawlLRU.fwdBucket) == string(tokenLRU.fwdBucket) {
		t.Fatal("crawl and token caches share a forward bucket")
	}

	// Behavior parity: touch three entries in each, evict to two, and
	// observe the same coldest-eviction outcome in both.
	ids := make([]core.Identity, 3)
	for i := range ids {
		kp, _ := core.NewKeyPair()
		ids[i] = kp.Identity()
		if err := s.TouchCrawl(ids[i], int64(i)); err != nil {
			t.Fatalf("TouchCrawl %d: %v", i, err)
		}
		if err := s.LearnToken(string(ids[i]), int64(i)); err != nil {
			t.Fatalf("LearnToken %d: %v", i, err)
		}
	}
	// Count agrees across both caches before eviction.
	err := s.db.Update(func(tx *bolt.Tx) error {
		if got := crawlLRU.count(tx); got != 3 {
			t.Fatalf("crawl count: want 3, got %d", got)
		}
		if got := tokenLRU.count(tx); got != 3 {
			t.Fatalf("token count: want 3, got %d", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("count: %v", err)
	}

	// Evict both caches to two entries: the two hottest survive in each.
	// The crawl path drops the coldest identity's author bucket (its
	// side effect); the token path has no side effect beyond the LRU
	// entries.
	if err := s.EvictCrawl(2); err != nil {
		t.Fatalf("EvictCrawl: %v", err)
	}
	if err := s.LearnToken("tok-pressure", 100); err != nil {
		t.Fatalf("LearnToken pressure: %v", err)
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		// The token cap is the production default, so the pressure
		// learn alone does not evict; drive the generic evict-to-cap
		// directly with cap 2 so both caches run the same eviction.
		return tokenLRU.evictToCap(tx, 2, 100, nil, nil)
	})
	if err != nil {
		t.Fatalf("token evictToCap: %v", err)
	}

	// The coldest entry is gone from both caches; the hot ones remain.
	coldGoneCrawl, err := s.IsCrawlVisited(ids[0])
	if err != nil {
		t.Fatalf("IsCrawlVisited cold: %v", err)
	}
	if coldGoneCrawl {
		t.Fatal("crawl cache did not evict the coldest entry")
	}
	toks, err := s.KnownTokens()
	if err != nil {
		t.Fatalf("KnownTokens: %v", err)
	}
	tokSet := make(map[string]bool, len(toks))
	for _, tok := range toks {
		tokSet[tok] = true
	}
	if tokSet[string(ids[0])] {
		t.Fatal("token cache did not evict the coldest entry")
	}
	if !tokSet[string(ids[2])] || !tokSet["tok-pressure"] {
		t.Fatal("token cache evicted a hot entry")
	}

	// The crawl cache's eviction side effect removed the author bucket;
	// the token cache stores only LRU entries, so there is nothing
	// equivalent to check: its on-evict hook is nil by construction.
	err = s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketRemote).Bucket([]byte(ids[0])) != nil {
			t.Fatal("crawl eviction left the author bucket behind")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("bucket check: %v", err)
	}
}
