package store

import (
	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// The crawl cache is the follower-tracking LRU over the generic boltLRU,
// specialized with two policies: the keep predicate holds identities the
// user follows (their remote buckets hold synced PostLog events, not just
// cached profiles), and the on-evict hook deletes the evicted identity's
// remote bucket, which removes any follower-map record and Follow evidence
// it holds together.

// crawlLRU is the crawl cache LRU specialization.
var crawlLRU = newBoltLRU(bucketCrawlLRU, bucketCrawlTS)

// TouchCrawl records that an identity was crawled or synced at the given
// time, updating the crawl cache LRU. Identities already followed are
// still tracked (they are skipped at eviction time, not at touch time).
func (s *Store) TouchCrawl(id core.Identity, touched int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return crawlLRU.touch(tx, []byte(id), touched)
	})
}

// IsCrawlVisited reports whether an identity is tracked in the crawl LRU,
// i.e. it has been crawled in some run and not since evicted. The crawler
// uses this to avoid re-crawling an identity it has already reached.
func (s *Store) IsCrawlVisited(id core.Identity) (bool, error) {
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		found = crawlLRU.contains(tx, []byte(id))
		return nil
	})
	return found, err
}

// CrawlVisitedCount returns the number of identities currently tracked in
// the crawl LRU. The daemon logs this after each crawl.
func (s *Store) CrawlVisitedCount() (int, error) {
	var n int
	err := s.db.View(func(tx *bolt.Tx) error {
		n = crawlLRU.count(tx)
		return nil
	})
	return n, err
}

// EvictCrawl reduces the crawl cache to at most maxCache tracked identities
// by removing the coldest (oldest-touched) entries. A followed identity is
// never evicted: its remote bucket also holds synced PostLog events that
// are not cache, so the eviction skips it and re-inserts it at the hot end.
// Evicting a non-followed identity deletes its remote bucket, removing
// any follower-map record and Follow evidence together. When the map is
// current, the destroyed record is accounted for on the spot, so eviction
// stays O(1) and never forces a from-zero rebuild; while a rebuild is
// pending, the stale map is rebuilt from the surviving events on the next
// follower read.
func (s *Store) EvictCrawl(maxCache int) error {
	if maxCache <= 0 {
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		fsb := tx.Bucket(bucketFollowState)
		return crawlLRU.evictToCap(tx, maxCache, s.now64(),
			func(id []byte) bool {
				return decodeFollowState(fsb.Get(id)).followed
			},
			func(id []byte) {
				remote := tx.Bucket(bucketRemote)
				ab := remote.Bucket(id)
				if !s.followersNeedsRebuild.Load() && ab != nil {
					ownPub, err := ownPubkeyInTx(tx)
					if err != nil {
						return
					}
					// The bucket held the follower-map record and
					// its Follow evidence together; both are gone.
					if ab.Get(followMapKey(ownPub)) != nil {
						if n := followerCountTx(tx); n > 0 {
							_ = putFollowerCountTx(tx, n-1)
						}
					}
				}
				_ = remote.DeleteBucket(id)
			})
	})
}

// ResetCrawl clears the crawl LRU so the crawler can start fresh. It does
// not delete the crawled profile events from bucketRemote; a fresh crawl
// repopulates the LRU and evicts stale entries as needed.
func (s *Store) ResetCrawl() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return crawlLRU.reset(tx)
	})
}
