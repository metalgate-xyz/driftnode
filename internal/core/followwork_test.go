package core

import (
	"fmt"
	"testing"
)

// resetWorkBits restores the production difficulty after a test overrides it.
func resetWorkBits(t *testing.T) {
	t.Helper()
	t.Cleanup(func() { SetFollowWorkBitsForTesting(defaultFollowWorkBits) })
}

// The recipe hashes the author pubkey, target pubkey, and nonce, and
// requires a fixed count of leading zero bits on the digest, so grinding
// one nonce cannot amortize across targets or across a botnet's keypairs.
func TestVerifyFollowWork(t *testing.T) {
	author := [32]byte{1}
	target := [32]byte{2}
	nonce, err := GrindFollowNonce(author, target)
	if err != nil {
		t.Fatalf("grind: %v", err)
	}
	if !VerifyFollowWork(author, target, nonce) {
		t.Fatal("ground nonce does not verify for its (author, target) pair")
	}
	if VerifyFollowWork(author, target, nonce+1) == VerifyFollowWork(author, target, nonce+2) {
		// Not a strict requirement, but a sanity check that nearby nonces
		// are not systematically valid.
		if VerifyFollowWork(author, target, nonce+1) {
			t.Fatal("adjacent nonces unexpectedly valid")
		}
	}
}

// A nonce ground for one (author, target) pair does not verify for a
// different target at the hash level, not merely via signature
// verification. With a difficulty of defaultFollowWorkBits, a wrong pair
// passing requires a 2^bits collision, which is unreachable in test time.
func TestFollowWorkBoundToTarget(t *testing.T) {
	author := [32]byte{1}
	target := [32]byte{2}
	other := [32]byte{3}
	nonce, err := GrindFollowNonce(author, target)
	if err != nil {
		t.Fatalf("grind: %v", err)
	}
	if VerifyFollowWork(author, other, nonce) {
		t.Fatal("nonce verifies for a target it was not ground for")
	}
	if VerifyFollowWork(other, target, nonce) {
		t.Fatal("nonce verifies for an author it was not ground for")
	}
}

// GrindFollowNonce is deterministic: the lowest valid nonce is a pure
// function of (author, target) and the difficulty.
func TestGrindFollowNonceDeterministic(t *testing.T) {
	author := [32]byte{7}
	target := [32]byte{9}
	a, err := GrindFollowNonce(author, target)
	if err != nil {
		t.Fatalf("grind a: %v", err)
	}
	b, err := GrindFollowNonce(author, target)
	if err != nil {
		t.Fatalf("grind b: %v", err)
	}
	if a != b {
		t.Fatalf("grind is not deterministic: %d vs %d", a, b)
	}
}

// Sign refuses a Follow without a nonce meeting the difficulty. An Unfollow
// carries no work, so it is unaffected.
func TestSignRejectsWorklessFollow(t *testing.T) {
	kp, err := NewKeyPair()
	if err != nil {
		t.Fatalf("keypair: %v", err)
	}
	target := kp.Public
	var targetArr [32]byte
	copy(targetArr[:], target)

	if _, err := kp.Sign(Event{
		Kind: KindFollow, Log: ProfileLog, Timestamp: 1, Sequence: 1,
		Follow: &Follow{TargetPubkey: targetArr},
	}); err != ErrFollowWork {
		t.Fatalf("Sign without nonce: want ErrFollowWork, got %v", err)
	}

	nonce, err := GrindFollowNonce([32]byte(kp.Public), targetArr)
	if err != nil {
		t.Fatalf("grind: %v", err)
	}
	if _, err := kp.Sign(Event{
		Kind: KindFollow, Log: ProfileLog, Timestamp: 1, Sequence: 1,
		Follow: &Follow{TargetPubkey: targetArr, Nonce: nonce},
	}); err != nil {
		t.Fatalf("Sign with ground nonce: %v", err)
	}

	// An Unfollow needs no work.
	if _, err := kp.Sign(Event{
		Kind: KindUnfollow, Log: ProfileLog, Timestamp: 1, Sequence: 1,
		Follow: &Follow{TargetPubkey: targetArr},
	}); err != nil {
		t.Fatalf("Sign unfollow: %v", err)
	}
}

// The nonce round-trips through the canonical encoding, so a verified
// Follow carries its proof of work on the wire.
func TestFollowNonceRoundTrip(t *testing.T) {
	kp, _ := NewKeyPair()
	resetWorkBits(t)
	SetFollowWorkBitsForTesting(4)
	var target [32]byte
	copy(target[:], kp.Public)
	nonce, err := GrindFollowNonce([32]byte(kp.Public), target)
	if err != nil {
		t.Fatalf("grind: %v", err)
	}
	se, err := kp.Sign(Event{
		Kind: KindFollow, Log: ProfileLog, Timestamp: 1, Sequence: 1,
		Follow: &Follow{TargetPubkey: target, Nonce: nonce},
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	var decoded Event
	if err := CanonicalDecode(mustEncode(t, se.Event), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Follow == nil || decoded.Follow.Nonce != nonce {
		t.Fatalf("nonce not round-tripped: %+v", decoded.Follow)
	}
	if err := se.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func mustEncode(t *testing.T, ev Event) []byte {
	t.Helper()
	b, err := CanonicalEncode(ev)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

// VerifyFollowWork reads the difficulty set at the time of the call, so the
// test seam changes both grind and verify consistently.
func TestSetFollowWorkBitsForTesting(t *testing.T) {
	resetWorkBits(t)
	SetFollowWorkBitsForTesting(1)
	author := [32]byte{5}
	target := [32]byte{6}
	nonce, err := GrindFollowNonce(author, target)
	if err != nil {
		t.Fatalf("grind at 1 bit: %v", err)
	}
	if !VerifyFollowWork(author, target, nonce) {
		t.Fatal("verify at 1 bit")
	}
	// Raising the difficulty invalidates the same nonce.
	SetFollowWorkBitsForTesting(defaultFollowWorkBits)
	if VerifyFollowWork(author, target, nonce) {
		t.Fatal("nonce ground at 1 bit verifies at production difficulty")
	}
}

// A zero-nonce Follow is indistinguishable from a workless one and must be
// rejected, closing the trivial bypass.
func TestFollowWorkDeterministicGrindBytes(t *testing.T) {
	author := [32]byte{1}
	target := [32]byte{2}
	nonce, err := GrindFollowNonce(author, target)
	if err != nil {
		t.Fatalf("grind: %v", err)
	}
	if nonce == 0 {
		t.Skip("nonce 0 happens to be valid; nothing to assert")
	}
	if VerifyFollowWork(author, target, 0) {
		t.Fatal("zero nonce verifies")
	}
}

// A benchmark that grinds one nonce per follow over a spread of author and
// target keys, used to pin the difficulty so one follow costs roughly the
// target grind time on the pin machine. Each sub-benchmark measures one
// candidate difficulty; the pinned constant is the bit count whose ns/op
// lands closest to the target (see defaultFollowWorkBits).
func BenchmarkFollowWork(b *testing.B) {
	const spread = 8
	authors := make([][32]byte, spread)
	targets := make([][32]byte, spread)
	for i := 0; i < spread; i++ {
		authors[i] = [32]byte{byte(i + 1)}
		authors[i][31] = 0xAA
		targets[i] = [32]byte{byte(i + 1)}
		targets[i][31] = 0xBB
	}
	for bits := 16; bits <= 28; bits += 2 {
		b.Run(fmt.Sprintf("bits=%d", bits), func(b *testing.B) {
			SetFollowWorkBitsForTesting(int32(bits))
			defer SetFollowWorkBitsForTesting(defaultFollowWorkBits)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := GrindFollowNonce(authors[i%spread], targets[i%spread]); err != nil {
					b.Fatalf("grind: %v", err)
				}
			}
		})
	}
}
