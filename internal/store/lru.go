package store

import (
	"encoding/binary"

	bolt "go.etcd.io/bbolt"
)

// boltLRU is a generic, tx-scoped LRU over two bbolt buckets: a forward
// bucket keyed by 8-byte big-endian touch time plus the id (sorted
// iteration yields the coldest first), and a reverse bucket mapping id to
// touch time (O(1) touch, membership, and count). All methods take the
// transaction to use, so callers compose LRU updates into larger
// transactions.
//
// evictToCap tracks count with a local counter rather than re-reading
// Stats().KeyN in the loop: within a write transaction that value reflects
// on-disk pages, not in-memory deletes, so it stays stale until commit and
// would over-evict.
type boltLRU struct {
	fwdBucket []byte
	revBucket []byte
}

// newBoltLRU returns the LRU specialized over the named forward and
// reverse buckets. Both buckets must exist in every transaction passed to
// the LRU's methods.
func newBoltLRU(fwd, rev []byte) boltLRU {
	return boltLRU{fwdBucket: fwd, revBucket: rev}
}

// lruFwdKey builds the forward composite key: 8-byte big-endian touch time
// followed by the id bytes, so the coldest entry sorts first and same-
// nanosecond touches disambiguate by id.
func lruFwdKey(touched int64, id []byte) []byte {
	k := make([]byte, 8+len(id))
	binary.BigEndian.PutUint64(k, uint64(touched))
	copy(k[8:], id)
	return k
}

// touch moves id to the hot end at the given time within tx.
func (l boltLRU) touch(tx *bolt.Tx, id []byte, touched int64) error {
	lru := tx.Bucket(l.fwdBucket)
	ts := tx.Bucket(l.revBucket)
	if raw := ts.Get(id); raw != nil {
		_ = lru.Delete(lruFwdKey(int64(binary.BigEndian.Uint64(raw)), id))
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(touched))
	if err := lru.Put(lruFwdKey(touched, id), []byte{}); err != nil {
		return err
	}
	return ts.Put(id, buf[:])
}

// contains reports whether id is currently tracked within tx.
func (l boltLRU) contains(tx *bolt.Tx, id []byte) bool {
	return tx.Bucket(l.revBucket).Get(id) != nil
}

// count returns the number of tracked ids within tx.
func (l boltLRU) count(tx *bolt.Tx) int {
	return tx.Bucket(l.revBucket).Stats().KeyN
}

// remove drops id from the LRU within tx, if tracked.
func (l boltLRU) remove(tx *bolt.Tx, id []byte) error {
	lru := tx.Bucket(l.fwdBucket)
	ts := tx.Bucket(l.revBucket)
	if raw := ts.Get(id); raw != nil {
		if err := lru.Delete(lruFwdKey(int64(binary.BigEndian.Uint64(raw)), id)); err != nil {
			return err
		}
	}
	return ts.Delete(id)
}

// evictToCap removes the coldest entries until at most cap ids remain.
// keep is consulted for each eviction candidate: a kept entry is re-touched
// to the hot end (now) and not counted as removed. onEvict runs for each
// actual removal. Either hook may be nil.
func (l boltLRU) evictToCap(tx *bolt.Tx, cap int, now int64, keep func(id []byte) bool, onEvict func(id []byte)) error {
	if cap <= 0 {
		return nil
	}
	lru := tx.Bucket(l.fwdBucket)
	ts := tx.Bucket(l.revBucket)
	// Stats().KeyN is stale inside a write transaction (it reflects
	// committed pages, not in-memory mutations), and callers compose the
	// touch and the evict into one transaction, so the count cannot be
	// seeded from it. Count the reverse bucket exactly instead: one O(n)
	// scan with n bounded by the cap, then track removals locally. Any
	// path that adds or removes an entry re-runs this function, so the
	// exact count is recomputed rather than drifting.
	count := 0
	if err := ts.ForEach(func(_, _ []byte) error {
		count++
		return nil
	}); err != nil {
		return err
	}
	for count > cap {
		c := lru.Cursor()
		k, _ := c.First()
		if k == nil {
			break
		}
		if len(k) <= 8 {
			// A malformed forward key cannot yield an id; drop it so
			// the loop always makes progress.
			_ = lru.Delete(k)
			count--
			continue
		}
		id := append([]byte(nil), k[8:]...)
		if keep != nil && keep(id) {
			_ = lru.Delete(k)
			if err := lru.Put(lruFwdKey(now, id), []byte{}); err != nil {
				return err
			}
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], uint64(now))
			_ = ts.Put(id, buf[:])
			continue
		}
		if onEvict != nil {
			onEvict(id)
		}
		_ = lru.Delete(k)
		_ = ts.Delete(id)
		count--
	}
	return nil
}

// reset clears every tracked entry within tx. onEvict is not called: the
// caller decides what a wholesale reset means for specialized state.
func (l boltLRU) reset(tx *bolt.Tx) error {
	lru := tx.Bucket(l.fwdBucket)
	ts := tx.Bucket(l.revBucket)
	if err := lru.ForEach(func(k, _ []byte) error { return lru.Delete(k) }); err != nil {
		return err
	}
	return ts.ForEach(func(k, _ []byte) error { return ts.Delete(k) })
}
