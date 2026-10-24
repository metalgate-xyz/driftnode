package core

import (
	"encoding/binary"
	"errors"
	"sync/atomic"

	"lukechampine.com/blake3"
)

// Follow work is proof that grinding happened before the Follow was
// signed. A Follow event carries a Nonce covered by the signature, so the
// work is bound to one (author, target) pair: the recipe is BLAKE3-256
// over the author pubkey, the target pubkey, and the nonce, and the digest
// must show a fixed count of leading zero bits. Grinding one nonce cannot
// amortize across targets or across a botnet's keypairs.

// defaultFollowWorkBits is the difficulty of the Follow proof of work: the
// number of leading zero bits required on the BLAKE3-256 digest. It is
// pinned from BenchmarkFollowWork on the reference machine (Apple M3 Pro,
// ~190ns per grind hash) so one follow costs roughly 100ms there: friction,
// not a hard barrier. Optimized hardware grinds faster, and the
// follower-map cap is the real bound on minted identities.
const defaultFollowWorkBits = 19

// ErrFollowWork is returned by Sign when a Follow carries no nonce meeting
// the difficulty. The author side enforces it so a legitimate author never
// signs a workless Follow.
var ErrFollowWork = errors.New("follow missing proof of work")

// followWorkBits is the active difficulty. Production always reads
// defaultFollowWorkBits; tests override it via
// SetFollowWorkBitsForTesting to keep fixtures cheap.
var followWorkBits atomic.Int32

func init() {
	followWorkBits.Store(defaultFollowWorkBits)
}

// SetFollowWorkBitsForTesting overrides the Follow work difficulty. It
// exists so test suites that seed follow fixtures do not pay the
// production grind per event. Production code never calls it.
func SetFollowWorkBitsForTesting(bits int32) { followWorkBits.Store(bits) }

// DefaultFollowWorkBits returns the pinned production difficulty, so a test
// that lowered it can restore the exact default.
func DefaultFollowWorkBits() int32 { return defaultFollowWorkBits }

// followWorkHash returns the BLAKE3-256 digest binding author, target, and
// nonce into one proof of work.
func followWorkHash(author, target [32]byte, nonce uint64) [32]byte {
	var buf [72]byte
	copy(buf[:32], author[:])
	copy(buf[32:64], target[:])
	binary.BigEndian.PutUint64(buf[64:], nonce)
	return blake3.Sum256(buf[:])
}

// leadingZeroBits counts the leading zero bits of a 32-byte digest.
func leadingZeroBits(d [32]byte) int {
	bits := 0
	for _, b := range d {
		if b == 0 {
			bits += 8
			continue
		}
		for mask := byte(0x80); mask != 0 && b&mask == 0; mask >>= 1 {
			bits++
		}
		break
	}
	return bits
}

// GrindFollowNonce finds the lowest nonce whose proof-of-work digest meets
// the difficulty, binding the work to the (author, target) pair. The result
// is deterministic: the lowest valid nonce is a pure function of the pair
// and the difficulty.
func GrindFollowNonce(author, target [32]byte) (uint64, error) {
	need := int(followWorkBits.Load())
	for nonce := uint64(0); ; nonce++ {
		if d := followWorkHash(author, target, nonce); leadingZeroBits(d) >= need {
			return nonce, nil
		}
		if nonce == ^uint64(0) {
			return 0, ErrFollowWork
		}
	}
}

// VerifyFollowWork reports whether the nonce meets the difficulty for the
// (author, target) pair.
func VerifyFollowWork(author, target [32]byte, nonce uint64) bool {
	return leadingZeroBits(followWorkHash(author, target, nonce)) >= int(followWorkBits.Load())
}
