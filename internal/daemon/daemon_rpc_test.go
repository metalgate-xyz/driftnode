package daemon

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"driftnode/internal/core"
	"driftnode/internal/proto/driftnodepb"
	syncproto "driftnode/internal/sync"
)

func TestWhoami(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	cl, _ := testClient(t, sock)
	resp, err := cl.Whoami(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("Whoami: %v", err)
	}
	if resp.Identity == "" {
		t.Fatal("missing identity in result")
	}
}

func TestZens(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	d.upsertZen("p1", "native", "connected")
	d.upsertZen("p2", "browser", "connected")

	cl, _ := testClient(t, sock)
	resp, err := cl.Zens(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("Zens: %v", err)
	}
	if len(resp.Zens) != 2 {
		t.Fatalf("want 2 zens, got %d", len(resp.Zens))
	}
}

func TestVerifyZens(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	peerKP, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	peerID := peerKP.Identity()
	d.upsertZen("p1", "native", "connected")
	if err := s.PutRouting(peerID, "p1"); err != nil {
		t.Fatalf("PutRouting: %v", err)
	}

	cl, _ := testClient(t, sock)

	// Before verify, the zen is not marked verified.
	resp, err := cl.Zens(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("zens: %v", err)
	}
	if len(resp.Zens) != 1 {
		t.Fatalf("want 1 zen, got %d", len(resp.Zens))
	}
	if resp.Zens[0].Verified {
		t.Fatal("zen should not be verified before verify RPC")
	}

	// Verify the peer identity out-of-band.
	if _, err := cl.Verify(context.Background(), &driftnodepb.IdentityReq{Identity: string(peerID)}); err != nil {
		t.Fatalf("verify: %v", err)
	}

	// After verify, the zen is marked verified.
	resp, err = cl.Zens(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("zens after verify: %v", err)
	}
	if !resp.Zens[0].Verified {
		t.Fatal("zen should be verified after verify RPC")
	}

	// Unverify clears the flag.
	if _, err := cl.Unverify(context.Background(), &driftnodepb.IdentityReq{Identity: string(peerID)}); err != nil {
		t.Fatalf("unverify: %v", err)
	}
	resp, _ = cl.Zens(context.Background(), &driftnodepb.Empty{})
	if resp.Zens[0].Verified {
		t.Fatal("zen should not be verified after unverify")
	}
}

// TestZensShowsIdentityWithoutName covers a discovered zen whose exchange
// ref carried an identity but no name hint (the common case before the
// crawler fetches the ProfileLog). The zens RPC must surface the identity
// rather than leaving it blank.
func TestZensShowsIdentityWithoutName(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	peerKP, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	peerID := peerKP.Identity()

	d.learnZenRef(syncproto.ZenRef{Token: "tok-no-name", Identity: peerID})

	cl, _ := testClient(t, sock)
	resp, err := cl.Zens(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("zens: %v", err)
	}
	if len(resp.Zens) != 1 {
		t.Fatalf("want 1 zen, got %d", len(resp.Zens))
	}
	if resp.Zens[0].Identity != string(peerID) {
		t.Fatalf("identity: want %s, got %q", peerID, resp.Zens[0].Identity)
	}
	if resp.Zens[0].Name != "" {
		t.Fatalf("name should be empty, got %q", resp.Zens[0].Name)
	}
}

// TestZensRehydratedOnRestart proves the zens map is rebuilt from the
// persisted routing table when the daemon starts, so the CLI/TUI show the
// known network immediately after a restart instead of an empty list.
func TestZensRehydratedOnRestart(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	peerKP, err := core.NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	peerID := peerKP.Identity()
	peerTok := "tok-peer"
	if err := s.PutRouting(peerID, peerTok); err != nil {
		t.Fatalf("PutRouting: %v", err)
	}

	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cl, _ := testClient(t, sock)
	resp, err := cl.Zens(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("zens: %v", err)
	}
	if len(resp.Zens) != 1 {
		t.Fatalf("want 1 rehydrated zen, got %d", len(resp.Zens))
	}
	if resp.Zens[0].Id != peerTok {
		t.Fatalf("id: want %q, got %q", peerTok, resp.Zens[0].Id)
	}
	if resp.Zens[0].Identity != string(peerID) {
		t.Fatalf("identity: want %s, got %q", peerID, resp.Zens[0].Identity)
	}
	if resp.Zens[0].Status != "connecting" {
		t.Fatalf("status: want connecting, got %q", resp.Zens[0].Status)
	}

	d.Stop()
}

func TestStatus(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	cl, _ := testClient(t, sock)
	resp, err := cl.Status(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if !resp.Running {
		t.Fatal("not running")
	}
}

func TestSync(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	cl, _ := testClient(t, sock)
	resp, err := cl.Sync(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if resp.Status != "triggered" {
		t.Fatalf("unexpected status: %v", resp.Status)
	}
}

func TestDialClientNotRunning(t *testing.T) {
	sock := testSocketPath(t)
	_, _, err := DialClient(sock)
	if err == nil {
		t.Fatal("expected error when daemon not running")
	}
}

func TestFollowParamsMarshal(t *testing.T) {
	s := newTestStore(t)
	kp := initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	d.Start(sock)
	defer d.Stop()

	cl, _ := testClient(t, sock)
	resp, err := cl.Follow(context.Background(), &driftnodepb.FollowReq{
		Target:     string(kp.Identity()),
		Passphrase: "pass",
	})
	if err != nil {
		t.Fatalf("Follow: %v", err)
	}
	if resp.Followed == "" {
		t.Fatalf("unexpected empty followed: %v", resp)
	}
}

func TestStop(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cl, _ := testClient(t, sock)
	resp, err := cl.Stop(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if resp.Status != "stopping" {
		t.Fatalf("unexpected stop status: %v", resp.Status)
	}

	select {
	case <-d.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("daemon did not stop after stop RPC")
	}

	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Fatalf("socket file still exists: %v", err)
	}
}

// statusUnlocked reads the unlocked field from a status response.
func statusUnlocked(t *testing.T, cl driftnodepb.DriftnodeClient) bool {
	t.Helper()
	resp, err := cl.Status(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	return resp.Unlocked
}

func TestUnlockPostWithoutPassphrase(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	cl, _ := testClient(t, sock)

	if statusUnlocked(t, cl) {
		t.Fatal("key should be locked before unlock")
	}
	if _, err := cl.Post(context.Background(), &driftnodepb.PostReq{Text: "no"}); err == nil {
		t.Fatal("keyless post should fail before unlock")
	}

	// Wrong passphrase is rejected and leaves the key locked.
	if _, err := cl.Unlock(context.Background(), &driftnodepb.UnlockReq{Passphrase: "wrong"}); err == nil {
		t.Fatal("wrong passphrase should fail")
	}
	if statusUnlocked(t, cl) {
		t.Fatal("key should stay locked after wrong passphrase")
	}

	// Correct passphrase unlocks and lets post omit it.
	if _, err := cl.Unlock(context.Background(), &driftnodepb.UnlockReq{Passphrase: "pass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if !statusUnlocked(t, cl) {
		t.Fatal("key should be unlocked after unlock RPC")
	}
	resp, err := cl.Post(context.Background(), &driftnodepb.PostReq{Text: "hello"})
	if err != nil {
		t.Fatalf("keyless post: %v", err)
	}
	if resp.EventId == "" {
		t.Fatalf("missing event_id: %v", resp)
	}

	// Lock clears the key; keyless post fails again.
	if _, err := cl.Lock(context.Background(), &driftnodepb.Empty{}); err != nil {
		t.Fatalf("lock: %v", err)
	}
	if statusUnlocked(t, cl) {
		t.Fatal("key should be locked after lock RPC")
	}
	if _, err := cl.Post(context.Background(), &driftnodepb.PostReq{Text: "no"}); err == nil {
		t.Fatal("keyless post should fail after lock")
	}
}

func TestPostWithPassphraseUnlocksImplicitly(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	cl, _ := testClient(t, sock)
	resp, err := cl.Post(context.Background(), &driftnodepb.PostReq{Text: "one", Passphrase: "pass"})
	if err != nil {
		t.Fatalf("post with passphrase: %v", err)
	}
	if resp.EventId == "" {
		t.Fatalf("missing event_id: %v", resp)
	}
	if !statusUnlocked(t, cl) {
		t.Fatal("post with passphrase should unlock implicitly")
	}
	if _, err := cl.Post(context.Background(), &driftnodepb.PostReq{Text: "two"}); err != nil {
		t.Fatalf("keyless post after implicit unlock: %v", err)
	}
}

func TestIdleLockReapsKey(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	d.SetIdleLock(20 * time.Millisecond)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	cl, _ := testClient(t, sock)
	if _, err := cl.Unlock(context.Background(), &driftnodepb.UnlockReq{Passphrase: "pass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if !statusUnlocked(t, cl) {
		t.Fatal("key should be unlocked")
	}
	time.Sleep(40 * time.Millisecond)

	if _, err := cl.Post(context.Background(), &driftnodepb.PostReq{Text: "no"}); err == nil {
		t.Fatal("keyless post should fail after idle-lock reaped the key")
	}
	if statusUnlocked(t, cl) {
		t.Fatal("key should be reaped after idle-lock expiry")
	}
}

func TestProfileDetailRPC(t *testing.T) {
	s := newTestStore(t)
	kp := initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	cl, _ := testClient(t, sock)
	if _, err := cl.Unlock(context.Background(), &driftnodepb.UnlockReq{Passphrase: "pass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if _, err := cl.Profile(context.Background(), &driftnodepb.ProfileReq{Name: "alice"}); err != nil {
		t.Fatalf("profile RPC: %v", err)
	}
	if _, err := cl.Detail(context.Background(), &driftnodepb.DetailReq{Bio: "wanderer", Location: "Wonderland"}); err != nil {
		t.Fatalf("detail RPC: %v", err)
	}

	profEvents, err := s.OwnEvents(core.ProfileLog)
	if err != nil {
		t.Fatalf("read profile log: %v", err)
	}
	if len(profEvents) != 1 || profEvents[0].Event.Profile == nil || profEvents[0].Event.Profile.DisplayName != "alice" {
		t.Fatalf("profile event mismatch: %+v", profEvents)
	}
	detEvents, err := s.OwnEvents(core.DetailLog)
	if err != nil {
		t.Fatalf("read detail log: %v", err)
	}
	if len(detEvents) != 1 || detEvents[0].Event.Detail == nil {
		t.Fatalf("detail event missing: %+v", detEvents)
	}
	det := detEvents[0].Event.Detail
	if det.Bio != "wanderer" || det.Location != "Wonderland" {
		t.Fatalf("detail fields mismatch: %+v", det)
	}
	_ = kp
}

func TestFollowsAndFollowersRPC(t *testing.T) {
	s := newTestStore(t)
	ownKP := initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	cl, _ := testClient(t, sock)
	if _, err := cl.Unlock(context.Background(), &driftnodepb.UnlockReq{Passphrase: "pass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	targetA, _ := core.NewKeyPair()
	targetB, _ := core.NewKeyPair()
	for _, target := range []core.Identity{targetA.Identity(), targetB.Identity()} {
		if _, err := cl.Follow(context.Background(), &driftnodepb.FollowReq{Target: string(target)}); err != nil {
			t.Fatalf("follow %s: %v", target, err)
		}
	}

	resp, err := cl.Follows(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("follows: %v", err)
	}
	if len(resp.Identities) != 2 {
		t.Fatalf("follows: want 2, got %d", len(resp.Identities))
	}

	// A follower follows us: store their Follow event via the sync path.
	followerKP, _ := core.NewKeyPair()
	followEvent, err := followerKP.Sign(core.Event{
		Kind:      core.KindFollow,
		Log:       core.ProfileLog,
		Timestamp: 100,
		Sequence:  1,
		Follow:    &core.Follow{TargetPubkey: [32]byte(ownKP.Public)},
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if _, err := s.PutCrawledEvent(followEvent, 1); err != nil {
		t.Fatalf("PutCrawledEvent: %v", err)
	}

	fresp, err := cl.Followers(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("followers: %v", err)
	}
	if len(fresp.Identities) != 1 {
		t.Fatalf("followers: want 1, got %d", len(fresp.Identities))
	}
}

// TestRotateKey verifies that rotate-key swaps the listener to a fresh
// address token and persists the new key, so the token survives a restart.
func TestRotateKey(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	d.mu.Lock()
	oldListener := d.tcListener
	d.mu.Unlock()
	if oldListener == nil {
		t.Skip("tailcat listener unavailable in this environment")
	}
	oldAddr := string(oldListener.Addr())

	cl, _ := testClient(t, sock)
	resp, err := cl.RotateKey(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("rotate-key: %v", err)
	}
	if resp.Token == "" {
		t.Fatal("rotate-key returned empty token")
	}
	if resp.Token == oldAddr {
		t.Fatal("rotate-key did not change the address token")
	}

	stored, ok, _ := s.TransportKey()
	if !ok {
		t.Fatal("transport key not persisted after rotate")
	}
	if string(stored) == "" {
		t.Fatal("persisted transport key is empty")
	}

	whoami, err := cl.Whoami(context.Background(), &driftnodepb.Empty{})
	if err != nil {
		t.Fatalf("whoami after rotate: %v", err)
	}
	if whoami.Token != resp.Token {
		t.Fatalf("whoami token after rotate: want %s, got %s", resp.Token, whoami.Token)
	}
}

// subscribeStream opens a subscribe stream and reads one event. It fails the
// test on error.
func subscribeStream(t *testing.T, cl driftnodepb.DriftnodeClient) (driftnodepb.Driftnode_SubscribeClient, *driftnodepb.Event) {
	t.Helper()
	stream, err := cl.Subscribe(context.Background(), &driftnodepb.SubscribeReq{})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv subscribe event: %v", err)
	}
	return stream, ev
}

// TestSubscribeSnapshot verifies a subscribe stream receives a snapshot
// carrying all four panels plus status.
func TestSubscribeSnapshot(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	cl, _ := testClient(t, sock)
	_, ev := subscribeStream(t, cl)
	snap, ok := ev.Kind.(*driftnodepb.Event_Snapshot)
	if !ok {
		t.Fatalf("first event: want Snapshot, got %T", ev.Kind)
	}
	if snap.Snapshot == nil {
		t.Fatal("nil snapshot")
	}
	if snap.Snapshot.Status == nil {
		t.Fatal("snapshot missing status")
	}
}

// TestSubscribeSnapshotFeedIsCapped verifies the snapshot does not send the
// entire feed when it is large. The TUI only renders the visible window, so
// sending millions of posts would freeze the client; the snapshot is capped
// and newer posts arrive as diffs.
func TestSubscribeSnapshotFeedIsCapped(t *testing.T) {
	s := newTestStore(t)
	kp := initTestIdentity(t, s)
	d := New(s, nil)
	d.SetUnlockedKey(kp)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	cl, _ := testClient(t, sock)
	for i := 0; i < snapshotFeedLimit+50; i++ {
		if _, err := cl.Post(context.Background(), &driftnodepb.PostReq{Text: "post"}); err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	_, ev := subscribeStream(t, cl)
	snap, ok := ev.Kind.(*driftnodepb.Event_Snapshot)
	if !ok {
		t.Fatalf("first event: want Snapshot, got %T", ev.Kind)
	}
	if len(snap.Snapshot.Feed) > snapshotFeedLimit {
		t.Fatalf("snapshot feed cap: want <= %d, got %d", snapshotFeedLimit, len(snap.Snapshot.Feed))
	}

	// A second subscribe must return the same feed from the cache without
	// rebuilding from disk.
	_, ev2 := subscribeStream(t, cl)
	snap2, _ := ev2.Kind.(*driftnodepb.Event_Snapshot)
	if len(snap2.Snapshot.Feed) != len(snap.Snapshot.Feed) {
		t.Fatalf("cached snapshot feed: want %d, got %d", len(snap.Snapshot.Feed), len(snap2.Snapshot.Feed))
	}
}

// TestSubscribeDiffOnPost verifies the daemon pushes a feed diff to a
// subscriber when a post is created, without the TUI re-fetching.
func TestSubscribeDiffOnPost(t *testing.T) {
	s := newTestStore(t)
	initTestIdentity(t, s)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	cl, _ := testClient(t, sock)
	stream, _ := subscribeStream(t, cl)

	// Unlock and post; a feed diff should arrive over the stream.
	if _, err := cl.Unlock(context.Background(), &driftnodepb.UnlockReq{Passphrase: "pass"}); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	// Drain the status diff from unlock so the next event is the feed diff.
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("drain status diff: %v", err)
	}
	if _, err := cl.Post(context.Background(), &driftnodepb.PostReq{Text: "hello-sub"}); err != nil {
		t.Fatalf("post: %v", err)
	}
	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv feed diff: %v", err)
	}
	diff, ok := ev.Kind.(*driftnodepb.Event_FeedDiff)
	if !ok {
		t.Fatalf("want FeedDiff, got %T", ev.Kind)
	}
	if len(diff.FeedDiff.Add) == 0 {
		t.Fatal("feed diff add is empty")
	}
	if diff.FeedDiff.Add[0].Text != "hello-sub" {
		t.Fatalf("feed diff text: want hello-sub, got %v", diff.FeedDiff.Add[0].Text)
	}
}

// TestSubscribeNoLostEvents verifies a subscriber does not miss a post that
// arrives while it is mid-subscribe. A correct implementation registers the
// subscriber before building the snapshot, so any post that lands between
// subscribe-open and first-event-read is either in the snapshot or delivered
// as a diff. The current ordering (snapshot -> send -> register) leaves a
// window where a post is neither.
//
// We maximize overlap by running many iterations of subscribe-while-posting
// concurrently; under the buggy ordering a post can land after the snapshot
// is built but before the subscriber is registered.
func TestSubscribeNoLostEvents(t *testing.T) {
	s := newTestStore(t)
	kp := initTestIdentity(t, s)
	d := New(s, nil)
	d.SetUnlockedKey(kp)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	const iterations = 50
	for i := 0; i < iterations; i++ {
		postCl, _ := testClient(t, sock)
		text := "race-post-" + itoa10(i)

		// Subscribe and immediately start a concurrent poster. The poster
		// races the snapshot/registration; a correct daemon delivers the
		// post either in the snapshot or as a subsequent diff.
		subCl, _ := testClient(t, sock)
		subCtx, subCancel := context.WithCancel(context.Background())
		stream, err := subCl.Subscribe(subCtx, &driftnodepb.SubscribeReq{})
		if err != nil {
			subCancel()
			t.Fatalf("subscribe: %v", err)
		}
		postDone := make(chan error, 1)
		go func() {
			_, err := postCl.Post(context.Background(), &driftnodepb.PostReq{Text: text})
			postDone <- err
		}()

		// Read the snapshot, then drain diffs looking for our post. The
		// contract is delivery in the snapshot feed OR a subsequent feed
		// diff; a correct Subscribe ordering must satisfy one of the two.
		snap, err := stream.Recv()
		if err != nil {
			subCancel()
			t.Fatalf("recv snapshot: %v", err)
		}
		snapEv, ok := snap.Kind.(*driftnodepb.Event_Snapshot)
		if !ok {
			subCancel()
			t.Fatalf("first event: want Snapshot, got %T", snap.Kind)
		}
		found := false
		if snapEv.Snapshot != nil {
			for _, it := range snapEv.Snapshot.Feed {
				if it.Text == text {
					found = true
				}
			}
		}
		if err := <-postDone; err != nil {
			subCancel()
			t.Fatalf("post: %v", err)
		}

		// Bounded drain: cancel the stream after a short window so a lost
		// event surfaces as a clean EOF instead of hanging forever.
		go time.AfterFunc(500*time.Millisecond, subCancel)
		for !found {
			ev, err := stream.Recv()
			if err != nil {
				break
			}
			if diff, ok := ev.Kind.(*driftnodepb.Event_FeedDiff); ok {
				for _, it := range diff.FeedDiff.Add {
					if it.Text == text {
						found = true
					}
				}
			}
		}
		subCancel()
		if !found {
			t.Fatalf("iteration %d: lost event %q (not in snapshot, not delivered as diff)", i, text)
		}
	}
}

// itoa10 formats a non-negative integer. Kept local to avoid a strconv import.
func itoa10(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// TestZensRPCConcurrentStatusMutation exercises the data race between the
// Zens RPC reading zenInfo fields after releasing d.mu and setZenStatus
// mutating the same struct under d.mu. The Zens handler collects *zenInfo
// pointers under the lock but reads their fields (Identity, Name, Status,
// Verified) after unlocking, so a concurrent status update races.
func TestZensRPCConcurrentStatusMutation(t *testing.T) {
	s := newTestStore(t)
	d := New(s, nil)
	sock := testSocketPath(t)
	if err := d.Start(sock); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	d.upsertZen("tok-a", "native_zen", "connecting")

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				d.setZenStatus("tok-a", "connecting")
				d.setZenStatus("tok-a", "connected")
			}
		}
	}()

	cl, _ := testClient(t, sock)
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := cl.Zens(context.Background(), &driftnodepb.Empty{}); err != nil {
			t.Fatalf("Zens: %v", err)
		}
	}
	close(stop)
}

// Compile-time check that io is used (stream EOF handling).
var _ = io.EOF
