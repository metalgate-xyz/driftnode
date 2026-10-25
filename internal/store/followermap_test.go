package store

import (
	"math/rand"
	"testing"
	"time"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// putOwnTargetedFollow stores a signed Follow event by follower targeting
// the local identity (identity must already be initialized).
func putOwnTargetedFollow(t *testing.T, s *Store, follower *core.KeyPair, seq uint64, ts int64, me [32]byte) *core.SignedEvent {
	t.Helper()
	nonce, err := core.GrindFollowNonce([32]byte(follower.Public), me)
	if err != nil {
		t.Fatalf("grind nonce: %v", err)
	}
	se, err := follower.Sign(core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: ts,
		Sequence:  seq,
		Follow:    &core.Follow{TargetPubkey: me, Nonce: nonce},
	})
	if err != nil {
		t.Fatalf("sign follow: %v", err)
	}
	if _, err := s.PutRemoteEvent(se, seq); err != nil {
		t.Fatalf("PutRemoteEvent: %v", err)
	}
	return se
}

// TestReceivedFollowersViaMapIncremental verifies the follower map is
// maintained incrementally inside PutRemoteEvent: the first follower read
// after open rebuilds from zero, then subsequent Follow and Unfollow events
// update the map through the ingest path without a further rebuild. The
// rebuild replays Follow and Unfollow in timestamp + sequence order, so a
// follower who later unfollows leaves the set.
func TestReceivedFollowersViaMapIncremental(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	alice, _ := core.NewKeyPair()
	bob, _ := core.NewKeyPair()
	dave, _ := core.NewKeyPair()
	var mePub [32]byte
	copy(mePub[:], me.Public)

	// First read with no events: the lazy rebuild runs and clears the
	// pending flag.
	if _, err := s.ReceivedFollowers(); err != nil {
		t.Fatalf("empty ReceivedFollowers: %v", err)
	}
	if s.followersNeedsRebuild.Load() {
		t.Fatal("rebuild flag not cleared by the empty read")
	}
	// Mark pending again so the next ingests defer maintenance; the
	// rebuild on the read below must observe the stored events.
	s.followersNeedsRebuild.Store(true)

	putOwnTargetedFollow(t, s, alice, 1, 5e9, mePub)
	putOwnTargetedFollow(t, s, bob, 1, 5e9, mePub)
	// Dave follows someone else, not me.
	daveNonce, _ := core.GrindFollowNonce([32]byte(dave.Public), [32]byte(alice.Public))
	daveFollow, err := dave.Sign(core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: 5e9,
		Sequence:  1,
		Follow:    &core.Follow{TargetPubkey: [32]byte(alice.Public), Nonce: daveNonce},
	})
	if err != nil {
		t.Fatalf("sign dave follow: %v", err)
	}
	if _, err := s.PutRemoteEvent(daveFollow, 1); err != nil {
		t.Fatalf("put dave follow: %v", err)
	}

	ids, err := s.ReceivedFollowers()
	if err != nil {
		t.Fatalf("ReceivedFollowers: %v", err)
	}
	want := map[core.Identity]bool{alice.Identity(): true, bob.Identity(): true}
	if len(ids) != len(want) {
		t.Fatalf("want %d followers, got %d (%v)", len(want), len(ids), ids)
	}
	for _, id := range ids {
		if !want[id] {
			t.Fatalf("unexpected follower %s", id)
		}
	}

	// Bob unfollows via a newer stored event.
	bobUnfollow, err := bob.Sign(core.Event{
		Kind:      core.KindUnfollow,
		Log:       core.ProfileLog,
		Timestamp: 6e9,
		Sequence:  2,
		Follow:    &core.Follow{TargetPubkey: mePub},
	})
	if err != nil {
		t.Fatalf("sign bob unfollow: %v", err)
	}
	if _, err := s.PutRemoteEvent(bobUnfollow, 2); err != nil {
		t.Fatalf("put bob unfollow: %v", err)
	}
	ids, err = s.ReceivedFollowers()
	if err != nil {
		t.Fatalf("ReceivedFollowers after unfollow: %v", err)
	}
	if len(ids) != 1 || ids[0] != alice.Identity() {
		t.Fatalf("after unfollow: want only alice, got %v", ids)
	}

	// ReceivedFollowEvents returns alice's Follow event with verifiable
	// signature.
	events, err := s.ReceivedFollowEvents()
	if err != nil {
		t.Fatalf("ReceivedFollowEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 follow event, got %d", len(events))
	}
	if events[0].Author != alice.Identity() {
		t.Fatalf("event author: want alice, got %s", events[0].Author)
	}
	if err := events[0].Verify(); err != nil {
		t.Fatalf("verify follow evidence: %v", err)
	}
}

// TestFollowerMapRebuildMatchesReplay verifies the lazy rebuild from zero
// equals the follower set computed by a full ordered replay (FollowSet.Apply
// semantics) over randomized stored Follow and Unfollow events.
func TestFollowerMapRebuildMatchesReplay(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	var mePub [32]byte
	copy(mePub[:], me.Public)
	core.SetFollowWorkBitsForTesting(1)
	defer core.SetFollowWorkBitsForTesting(core.DefaultFollowWorkBits())

	const followers = 30
	kps := make([]*core.KeyPair, followers)
	for i := range kps {
		kps[i], _ = core.NewKeyPair()
	}
	rng := rand.New(rand.NewSource(1))
	var expectedLog []core.SignedEvent
	for i := 0; i < 120; i++ {
		follower := kps[rng.Intn(followers)]
		follow := rng.Intn(3) != 0
		ts := int64(5e9 + i)
		kind := core.KindUnfollow
		nonce := uint64(0)
		if follow {
			kind = core.KindFollow
			n, err := core.GrindFollowNonce([32]byte(follower.Public), mePub)
			if err != nil {
				t.Fatalf("grind: %v", err)
			}
			nonce = n
		}
		se, err := follower.Sign(core.Event{
			Kind:      kind,
			Log:       core.ProfileLog,
			Timestamp: ts,
			Sequence:  uint64(i + 1),
			Follow:    &core.Follow{TargetPubkey: mePub, Nonce: nonce},
		})
		if err != nil {
			t.Fatalf("sign: %v", err)
		}
		if _, err := s.PutRemoteEvent(se, uint64(i+1)); err != nil {
			t.Fatalf("put: %v", err)
		}
		expectedLog = append(expectedLog, *se)
	}

	// Expected set: full ordered replay in (timestamp, sequence) order,
	// matching FollowSet.Apply per author.
	sorted := core.NewLog(expectedLog).SortedByTime()
	expected := make(map[core.Identity]bool)
	for _, se := range sorted {
		if se.Event.Follow == nil || se.Event.Follow.TargetPubkey != mePub {
			continue
		}
		if se.Event.Kind == core.KindFollow {
			expected[se.Author] = true
		} else if se.Event.Kind == core.KindUnfollow {
			delete(expected, se.Author)
		}
	}

	// Force a rebuild from zero and compare.
	s.followersNeedsRebuild.Store(true)
	ids, err := s.ReceivedFollowers()
	if err != nil {
		t.Fatalf("ReceivedFollowers: %v", err)
	}
	if len(ids) != len(expected) {
		t.Fatalf("rebuild: want %d followers, got %d (%v)", len(expected), len(ids), ids)
	}
	for _, id := range ids {
		if !expected[id] {
			t.Fatalf("rebuild claims follower %s absent from replay", id)
		}
	}
}

// TestFollowerMapOutOfOrderArrival verifies out-of-order delivery still
// converges to the replay order: an older Follow arriving after a newer
// one does not win the record, and a newer Unfollow removes it.
func TestFollowerMapOutOfOrderArrival(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	var mePub [32]byte
	copy(mePub[:], me.Public)

	alice, _ := core.NewKeyPair()
	// Newer Follow first.
	putOwnTargetedFollow(t, s, alice, 2, 8e9, mePub)
	// Older Follow arrives later: must not displace the record.
	putOwnTargetedFollow(t, s, alice, 1, 5e9, mePub)
	events, err := s.ReceivedFollowEvents()
	if err != nil {
		t.Fatalf("ReceivedFollowEvents: %v", err)
	}
	if len(events) != 1 || events[0].Event.Timestamp != 8e9 {
		t.Fatalf("older follow displaced the record: %+v", events)
	}

	// An Unfollow older than the record does not remove it.
	oldUnfollow, err := alice.Sign(core.Event{
		Kind:      core.KindUnfollow,
		Log:       core.ProfileLog,
		Timestamp: 5e9,
		Sequence:  3,
		Follow:    &core.Follow{TargetPubkey: mePub},
	})
	if err != nil {
		t.Fatalf("sign old unfollow: %v", err)
	}
	if _, err := s.PutRemoteEvent(oldUnfollow, 3); err != nil {
		t.Fatalf("put old unfollow: %v", err)
	}
	events, err = s.ReceivedFollowEvents()
	if err != nil {
		t.Fatalf("ReceivedFollowEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("older unfollow removed the record: %+v", events)
	}

	// A newer Unfollow removes it.
	newUnfollow, err := alice.Sign(core.Event{
		Kind:      core.KindUnfollow,
		Log:       core.ProfileLog,
		Timestamp: 9e9,
		Sequence:  4,
		Follow:    &core.Follow{TargetPubkey: mePub},
	})
	if err != nil {
		t.Fatalf("sign new unfollow: %v", err)
	}
	if _, err := s.PutRemoteEvent(newUnfollow, 4); err != nil {
		t.Fatalf("put new unfollow: %v", err)
	}
	events, err = s.ReceivedFollowEvents()
	if err != nil {
		t.Fatalf("ReceivedFollowEvents: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("newer unfollow did not remove the record: %+v", events)
	}
}

// TestFollowerMapCapDropped verifies that against a small overridden cap, the
// next unique follower's Follow event is dropped, existing records are kept,
// and the author is not flagged.
func TestFollowerMapCapDropped(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	var mePub [32]byte
	copy(mePub[:], me.Public)

	// Prime the map by reading once so maintenance becomes incremental.
	if _, err := s.ReceivedFollowers(); err != nil {
		t.Fatalf("prime read: %v", err)
	}

	followerA, _ := core.NewKeyPair()
	followerB, _ := core.NewKeyPair()
	putOwnTargetedFollow(t, s, followerA, 1, 5e9, mePub)
	putOwnTargetedFollow(t, s, followerB, 1, 5e9, mePub)

	ids, err := s.ReceivedFollowers()
	if err != nil {
		t.Fatalf("ReceivedFollowers: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("want 2 followers, got %d", len(ids))
	}

	// Force the counter to the cap, then the next unique follower is
	// dropped without flagging.
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return putFollowerCountTx(tx, followerCap)
	}); err != nil {
		t.Fatalf("force count to cap: %v", err)
	}
	followerC, _ := core.NewKeyPair()
	nonce, err := core.GrindFollowNonce([32]byte(followerC.Public), mePub)
	if err != nil {
		t.Fatalf("grind nonce: %v", err)
	}
	se, err := followerC.Sign(core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: 5e9,
		Sequence:  1,
		Follow:    &core.Follow{TargetPubkey: mePub, Nonce: nonce},
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	inserted, err := s.PutRemoteEvent(se, 1)
	if err != nil {
		t.Fatalf("PutRemoteEvent over cap: %v", err)
	}
	if inserted {
		t.Fatal("over-cap follower follow was stored")
	}
	flagged, err := s.IsFlagged(followerC.Identity())
	if err != nil {
		t.Fatalf("IsFlagged: %v", err)
	}
	if flagged {
		t.Fatal("over-cap follower author was flagged")
	}

	// Existing records kept.
	ids, err = s.ReceivedFollowers()
	if err != nil {
		t.Fatalf("ReceivedFollowers after cap: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("existing followers not kept: got %d", len(ids))
	}
}

// TestFollowerMapCrawlEvictionRemovesRecord verifies crawl eviction of a
// non-followed follower's author bucket removes the map record along with
// the evidence: no later read claims the follower.
func TestFollowerMapCrawlEvictionRemovesRecord(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	var mePub [32]byte
	copy(mePub[:], me.Public)

	alice, _ := core.NewKeyPair()
	putOwnTargetedFollow(t, s, alice, 1, 5e9, mePub)

	ids, err := s.ReceivedFollowers()
	if err != nil {
		t.Fatalf("ReceivedFollowers: %v", err)
	}
	if len(ids) != 1 || ids[0] != alice.Identity() {
		t.Fatalf("want alice as follower, got %v", ids)
	}

	// Alice is touched cold; a second identity touched hot fills the cap
	// of one and pushes Alice's author bucket out with it.
	bob, _ := core.NewKeyPair()
	if _, err := s.PutRemoteEvent(signRemoteFollowFor(t, bob, 1, 5e9+5, [32]byte(unknownPub(t))), 1); err != nil {
		t.Fatalf("seed bob: %v", err)
	}
	if err := s.TouchCrawl(alice.Identity(), 1); err != nil {
		t.Fatalf("TouchCrawl alice: %v", err)
	}
	if err := s.TouchCrawl(bob.Identity(), 2); err != nil {
		t.Fatalf("TouchCrawl bob: %v", err)
	}
	if err := s.EvictCrawl(1); err != nil {
		t.Fatalf("EvictCrawl: %v", err)
	}
	visited, err := s.IsCrawlVisited(alice.Identity())
	if err != nil {
		t.Fatalf("IsCrawlVisited: %v", err)
	}
	if visited {
		t.Fatal("alice was not evicted; the fixture did not exercise the eviction path")
	}

	ids, err = s.ReceivedFollowers()
	if err != nil {
		t.Fatalf("ReceivedFollowers after eviction: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("evicted follower still claimed: %v", ids)
	}
}

// TestFollowerMapLargeInputReadCost is the regression test for the follower
// read cost: the read must be proportional to the number of tracked authors
// (bounded by the follow and crawl caps), not to the number of stored
// events. It seeds many authors with many non-follower events each, asserts
// both correctness and that the read completes well under a second.
func TestFollowerMapLargeInputReadCost(t *testing.T) {
	s := newTestStore(t)
	me, _ := core.NewKeyPair()
	enc := core.DefaultKeyEncryption()
	ek, _ := enc.Encrypt(me.Private, []byte("pw"))
	if err := s.InitIdentity(me, ek); err != nil {
		t.Fatalf("init: %v", err)
	}
	var mePub [32]byte
	copy(mePub[:], me.Public)
	core.SetFollowWorkBitsForTesting(1)
	defer core.SetFollowWorkBitsForTesting(core.DefaultFollowWorkBits())

	// Many authors with many non-follower events each.
	const authors = 500
	const eventsPer = 40
	kps := make([]*core.KeyPair, authors)
	for i := range kps {
		kps[i], _ = core.NewKeyPair()
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		fb := tx.Bucket(bucketRemote)
		for i := 0; i < authors; i++ {
			ab, err := fb.CreateBucketIfNotExists([]byte(kps[i].Identity()))
			if err != nil {
				return err
			}
			other, _ := core.NewKeyPair()
			nonce, err := core.GrindFollowNonce([32]byte(kps[i].Public), [32]byte(other.Public))
			if err != nil {
				return err
			}
			for j := 0; j < eventsPer; j++ {
				se, err := kps[i].Sign(core.Event{
					Kind:      core.KindFollow,
					Log:       core.ProfileLog,
					Timestamp: int64(5e9 + i*eventsPer + j),
					Sequence:  uint64(j + 1),
					Follow:    &core.Follow{TargetPubkey: [32]byte(other.Public), Nonce: nonce},
				})
				if err != nil {
					return err
				}
				id, err := se.ID()
				if err != nil {
					return err
				}
				buf, err := core.CanonicalEncode(se)
				if err != nil {
					return err
				}
				if err := ab.Put(id[:], buf); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// A handful of followers targeting me, stored through the normal path.
	for i, f := range kps[:10] {
		nonce, nerr := core.GrindFollowNonce([32]byte(f.Public), mePub)
		if nerr != nil {
			t.Fatalf("grind: %v", nerr)
		}
		se, serr := f.Sign(core.Event{
			Kind:      core.KindFollow,
			Log:       core.ProfileLog,
			Timestamp: int64(6e9 + i),
			Sequence:  1,
			Follow:    &core.Follow{TargetPubkey: mePub, Nonce: nonce},
		})
		if serr != nil {
			t.Fatalf("sign: %v", serr)
		}
		if _, err := s.PutRemoteEvent(se, 1); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// First read: rebuild then point lookups. It must complete well under
	// a second even with tens of thousands of stored events.
	start := time.Now()
	ids, err := s.ReceivedFollowers()
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ReceivedFollowers: %v", err)
	}
	if len(ids) != 10 {
		t.Fatalf("want 10 followers, got %d", len(ids))
	}
	if elapsed > time.Second {
		t.Fatalf("follower read too slow: %v", elapsed)
	}
}

// unknownPub returns a throwaway pubkey distinct from all test identities,
// for seeding an unrelated event into an author bucket.
func unknownPub(t *testing.T) []byte {
	t.Helper()
	kp, _ := core.NewKeyPair()
	return kp.Public
}
