package store

import (
	"encoding/binary"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// crawlLRUKey builds the composite key for bucketCrawlLRU: an 8-byte
// big-endian last-touched timestamp (unix nanoseconds) followed by the
// identity bytes. bbolt iterates keys in sorted byte order, so the coldest
// (oldest-touched) author sorts first. The identity suffix disambiguates two
// authors touched in the same nanosecond.
func crawlLRUKey(touched int64, id core.Identity) []byte {
	k := make([]byte, 8+len(id))
	binary.BigEndian.PutUint64(k, uint64(touched))
	copy(k[8:], id)
	return k
}

// TouchCrawl records that an identity was crawled or synced at the given
// time, updating the crawl cache LRU. It removes any prior LRU entry for
// the identity and inserts a new one keyed by the touch time, so the
// identity moves to the hot end of the LRU. Identities already followed are
// still tracked (they are skipped at eviction time, not at touch time).
// A reverse index (bucketCrawlTS, identity -> touched ns) makes finding the
// prior forward key O(1), so a touch is O(1) rather than O(n) over the cache.
func (s *Store) TouchCrawl(id core.Identity, touched int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		lru := tx.Bucket(bucketCrawlLRU)
		ts := tx.Bucket(bucketCrawlTS)
		// Delete the prior forward entry using the reverse index, so the old
		// touch-time key does not linger and re-position the identity cold.
		if raw := ts.Get([]byte(id)); raw != nil {
			_ = lru.Delete(crawlLRUKey(int64(binary.BigEndian.Uint64(raw)), id))
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(touched))
		if err := lru.Put(crawlLRUKey(touched, id), []byte{}); err != nil {
			return err
		}
		return ts.Put([]byte(id), buf[:])
	})
}

// IsCrawlVisited reports whether an identity is tracked in the crawl LRU,
// i.e. it has been crawled in some run and not since evicted. The crawler
// uses this to avoid re-crawling an identity it has already reached.
func (s *Store) IsCrawlVisited(id core.Identity) (bool, error) {
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		found = tx.Bucket(bucketCrawlTS).Get([]byte(id)) != nil
		return nil
	})
	return found, err
}

// CrawlVisitedCount returns the number of identities currently tracked in
// the crawl LRU. The daemon logs this after each crawl.
func (s *Store) CrawlVisitedCount() (int, error) {
	var n int
	err := s.db.View(func(tx *bolt.Tx) error {
		n = tx.Bucket(bucketCrawlTS).Stats().KeyN
		return nil
	})
	return n, err
}

// EvictCrawl reduces the crawl cache to at most maxCache tracked identities
// by removing the coldest (oldest-touched) entries. A followed identity is
// never evicted: its remote bucket also holds synced PostLog events that are
// not cache, so the eviction skips it and re-inserts it at the hot end.
//
// Count is tracked with a local counter, not by re-reading Stats().KeyN in
// the loop. Within a write transaction, Stats().KeyN reflects the on-disk
// pages, not in-memory deletes, so it stays stale until commit and would
// cause the loop to over-evict. The counter is seeded once before any
// delete, then decremented on each removal below.
//
// If you add a path that adds or removes an LRU entry, keep the counter in
// sync: decrement on a real removal, leave it alone on a re-insert (the
// followed-author move below deletes one key and adds another, so the net
// count does not change).
func (s *Store) EvictCrawl(maxCache int) error {
	if maxCache <= 0 {
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		lru := tx.Bucket(bucketCrawlLRU)
		ts := tx.Bucket(bucketCrawlTS)
		remote := tx.Bucket(bucketRemote)
		fsb := tx.Bucket(bucketFollowState)
		count := int(ts.Stats().KeyN)
		for count > maxCache {
			c := lru.Cursor()
			k, _ := c.First()
			if k == nil {
				break
			}
			if len(k) <= 8 {
				_ = lru.Delete(k)
				count--
				continue
			}
			id := core.Identity(k[8:])
			// A followed identity's remote bucket holds synced PostLog
			// events, not just a cached profile, so do not evict it.
			// Re-insert at the hot end so it is not reconsidered next
			// round. Net count is unchanged: one key deleted, one added.
			st := decodeFollowState(fsb.Get([]byte(id)))
			if st.followed {
				now := s.now64()
				_ = lru.Delete(k)
				if err := lru.Put(crawlLRUKey(now, id), []byte{}); err != nil {
					return err
				}
				var buf [8]byte
				binary.BigEndian.PutUint64(buf[:], uint64(now))
				_ = ts.Put([]byte(id), buf[:])
				continue
			}
			_ = remote.DeleteBucket([]byte(id))
			_ = lru.Delete(k)
			_ = ts.Delete([]byte(id))
			count--
		}
		return nil
	})
}

// ResetCrawl clears the crawl LRU so the crawler can start fresh. It does
// not delete the crawled profile events from bucketRemote; a fresh crawl
// repopulates the LRU and evicts stale entries as needed.
func (s *Store) ResetCrawl() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		lru := tx.Bucket(bucketCrawlLRU)
		ts := tx.Bucket(bucketCrawlTS)
		if err := lru.ForEach(func(k, _ []byte) error { return lru.Delete(k) }); err != nil {
			return err
		}
		return ts.ForEach(func(k, _ []byte) error { return ts.Delete(k) })
	})
}
