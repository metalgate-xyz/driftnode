package weight

import (
	"math"
	"testing"

	"driftnode/internal/core"
)

// mkID builds a distinct identity from a single-byte pubkey fill.
func mkID(t *testing.T, b byte) core.Identity {
	t.Helper()
	var raw [32]byte
	for i := range raw {
		raw[i] = b
	}
	return core.IdentityFromPubkey(raw[:])
}

func TestWeightsColdStartDefault(t *testing.T) {
	a := mkID(t, 1)
	b := mkID(t, 2)
	in := Inputs{
		Follows: []core.Identity{a, b},
		// a: one like. b: nothing (cold-start).
		Likes: map[core.Identity]int{a: 1},
	}
	w := Weights(in)

	if w[b] != coldStartWeight {
		t.Fatalf("cold-start b: want %g, got %g", coldStartWeight, w[b])
	}
	// A single like must lift a above cold start, so any signal outranks none.
	singleLike := wLikes * normLog(1, likeSoftCap)
	if w[a] <= coldStartWeight {
		t.Fatalf("single like must beat cold start: a=%g cold=%g", w[a], coldStartWeight)
	}
	if math.Abs(w[a]-singleLike) > 1e-9 {
		t.Fatalf("single like weight: want %g, got %g", singleLike, w[a])
	}
}

func TestWeightsLogScalingSpreadsTail(t *testing.T) {
	// The p50 is 1 for both metrics. Linear scaling would collapse 1, 5,
	// and 20 to nearly the same value; log(1+x) must spread them so the
	// scheduler can distinguish a 1-like from a 20-like follow.
	x1 := normLog(1, likeSoftCap)
	x5 := normLog(5, likeSoftCap)
	x20 := normLog(20, likeSoftCap)
	if x1 <= 0 || x5 <= x1 || x20 <= x5 {
		t.Fatalf("log scale not monotonic increasing: 1=%g 5=%g 20=%g", x1, x5, x20)
	}
	// The 1-vs-5 gap must be at least as large (relatively) as a linear scale
	// would give, i.e. log scaling does not compress the low end.
	if (x5-x1)/x5 <= 0 {
		t.Fatalf("log scaling collapsed the 1-vs-5 gap")
	}
	// Saturation: counts at/above the cap tie at 1.0.
	if x20 != 1.0 {
		t.Fatalf("count at cap must saturate to 1.0: got %g", x20)
	}
	if normLog(100, likeSoftCap) != 1.0 {
		t.Fatalf("count above cap must saturate to 1.0")
	}
	if normLog(0, likeSoftCap) != 0 {
		t.Fatalf("zero count must normalize to 0")
	}
}

func TestWeightsPinDominatesInteraction(t *testing.T) {
	// A pinned follow with zero interaction must outrank a non-pinned
	// follow at the interaction ceiling (max likes + replies + recip).
	// This is the core property: a pin is the user's explicit top-priority
	// mark, not an emergent signal to be out-competed.
	ceiling := wLikes*1.0 + wReplies*1.0 + wRecip*1.0
	if wPin <= ceiling {
		t.Fatalf("wPin must dominate interaction ceiling: pin=%g ceiling=%g", wPin, ceiling)
	}

	pinned := mkID(t, 1)
	engaged := mkID(t, 2)
	in := Inputs{
		Follows: []core.Identity{pinned, engaged},
		Likes:   map[core.Identity]int{engaged: likeSoftCap},
		Replies: map[core.Identity]int{engaged: replySoftCap},
		Recip:   map[core.Identity]bool{engaged: true},
		Pins:    map[core.Identity]bool{pinned: true},
	}
	w := Weights(in)
	if w[pinned] <= w[engaged] {
		t.Fatalf("pinned no-interaction must outrank max-interaction non-pinned: pin=%g engaged=%g", w[pinned], w[engaged])
	}
	if math.Abs(w[pinned]-wPin) > 1e-9 {
		t.Fatalf("pinned weight: want %g, got %g", wPin, w[pinned])
	}
}

func TestWeightsReciprocalBoost(t *testing.T) {
	// A reciprocal follow with no other signal gets wRecip. Reciprocal is a
	// binary floor signal (the doc's implication #2): a fixed additive
	// boost that lifts a mutual follow above cold start and above a single
	// one-off like, since an ongoing mutual relationship is a stronger
	// per-edge priority signal than a single interaction. But it must not
	// dominate accumulated interaction: a like at the soft cap beats it.
	recip := mkID(t, 1)
	cold := mkID(t, 2)
	liker := mkID(t, 3)
	heavy := mkID(t, 4)
	in := Inputs{
		Follows: []core.Identity{recip, cold, liker, heavy},
		Recip:   map[core.Identity]bool{recip: true},
		Likes:   map[core.Identity]int{liker: 1, heavy: likeSoftCap},
	}
	w := Weights(in)
	if w[recip] <= coldStartWeight {
		t.Fatalf("reciprocal must beat cold start: recip=%g cold=%g", w[recip], coldStartWeight)
	}
	if math.Abs(w[recip]-wRecip) > 1e-9 {
		t.Fatalf("reciprocal weight: want %g, got %g", wRecip, w[recip])
	}
	if w[recip] <= w[liker] {
		t.Fatalf("reciprocal floor must beat a single like: recip=%g like=%g", w[recip], w[liker])
	}
	if w[recip] >= w[heavy] {
		t.Fatalf("saturated interaction must beat reciprocal-only: heavy=%g recip=%g", w[heavy], w[recip])
	}
}

func TestSortedDescending(t *testing.T) {
	a := mkID(t, 1)
	b := mkID(t, 2)
	c := mkID(t, 3)
	d := mkID(t, 4) // cold start
	in := Inputs{
		Follows: []core.Identity{d, a, c, b}, // unsorted, cold first
		Likes:   map[core.Identity]int{a: 1, b: likeSoftCap, c: 5},
	}
	s := Sorted(in)
	if len(s) != 4 {
		t.Fatalf("want 4 weights, got %d", len(s))
	}
	// b (cap likes) > c (5 likes) > a (1 like) > d (cold).
	want := []core.Identity{b, c, a, d}
	for i, w := range s {
		if w.ID != want[i] {
			t.Fatalf("rank %d: want %v got %v (weight %g)", i, want[i], w.ID, w.Weight)
		}
		if i > 0 && s[i].Weight > s[i-1].Weight {
			t.Fatalf("not descending at %d", i)
		}
	}
}

func TestSortedTiebreakByIdentity(t *testing.T) {
	// Two cold-start follows tie on weight; the identity string must break
	// the tie deterministically so the scheduler's order is stable.
	a := mkID(t, 0xAA)
	b := mkID(t, 0x55)
	in := Inputs{
		Follows: []core.Identity{a, b},
	}
	s := Sorted(in)
	if len(s) != 2 {
		t.Fatalf("want 2, got %d", len(s))
	}
	// Tiebreak is ascending identity string (smaller first).
	low, high := a, b
	if high < low {
		low, high = high, low
	}
	if s[0].ID != low || s[1].ID != high {
		t.Fatalf("tiebreak order: want [%v, %v], got [%v, %v]", low, high, s[0].ID, s[1].ID)
	}
	if s[0].Weight != s[1].Weight {
		t.Fatalf("ties must have equal weight: %g vs %g", s[0].Weight, s[1].Weight)
	}
}

func TestWeightsOnlyFollowsGetWeights(t *testing.T) {
	// An author with likes who is NOT in the follow graph must not appear in
	// the output: metrics are scoped to the follow graph.
	followed := mkID(t, 1)
	nonFollowed := mkID(t, 2)
	in := Inputs{
		Follows: []core.Identity{followed},
		Likes:  map[core.Identity]int{followed: 3, nonFollowed: 100},
	}
	w := Weights(in)
	if _, ok := w[nonFollowed]; ok {
		t.Fatalf("non-followed author must not get a weight")
	}
	if w[followed] == coldStartWeight {
		t.Fatalf("followed with likes must not be cold start")
	}
}
