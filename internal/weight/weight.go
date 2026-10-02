// Package weight computes the sync-priority weight for each followed
// identity from interaction metrics. The weight IS the sync priority:
// higher-weight follows are dialed first and every round by the scheduler
// (phase 6).
//
// The formula and its constants are settled here against the metric
// distributions measured in phase 4 (see docs/weighted-follow-graph-plan.md):
//
//   - Likes and replies are power-law, heavy-tailed interaction counts. Their
//     p50 is 1, so linear normalization collapses the bottom half to a single
//     value. log(1+x) scaling spreads the 1-vs-5-vs-20 distinction that
//     linear scaling erases, matching the shape of the data.
//   - Reciprocal follow and pins are binary (0 or 1). They are not
//     normalizable; they are a fixed additive boost, and a pin is a weight
//     floor that dominates any emergent interaction signal (a pin is the
//     user's explicit "top sync priority" mark).
//   - 20% of follows have zero interaction history (cold-start). They get a
//     small default weight so they stay in the sync set rather than dropping
//     to priority zero.
//
// Coefficients are source-code constants, not user-facing config.
package weight

import (
	"math"
	"sort"

	"driftnode/internal/core"
)

// Coefficients for the weighted sum, tuned against the phase-4 metric
// distributions. wPin dominates the sum of the interaction and reciprocal
// coefficients so a pinned follow with no interaction outranks any non-pinned
// follow: a pin is the user's explicit top-priority mark, not an emergent
// signal.
const (
	wLikes   = 0.4
	wReplies = 0.5
	wRecip   = 0.1
	wPin     = 1.5
)

// Soft caps for log-scaling likes and replies, set to the measured p99 of
// each metric in the 10k-zen power-law store. Counts at or above the cap
// saturate to a normalized 1.0, so the top ~1% tie at the ceiling while the
// log(1+x) scale spreads the long tail (1 vs 5 vs 20). Grounding the cap in
// the p99 rather than the raw max is what makes it a saturating soft cap
// instead of a single-author ceiling.
const (
	likeSoftCap  = 20 // outbound likes p99
	replySoftCap = 13 // outbound replies p99
)

// coldStartWeight is the weight assigned to a follow with no interaction
// signal at all (no likes, replies, reciprocal, or pin): the 20% cold-start
// cohort. It is positive so cold-start follows stay syncable, and below the
// contribution of a single like so any real signal lifts a follow above it.
const coldStartWeight = 0.05

// Inputs are the per-author interaction signals for one follow graph. The
// caller scopes every metric to the follow graph: likes and replies count
// only followed authors, reciprocal is a followed author who follows back,
// and pins are a subset of follows. Follows is the full graph so cold-start
// follows (absent from all metric maps) still receive a weight.
type Inputs struct {
	Follows []core.Identity        // the full follow graph, including cold-start follows
	Likes   map[core.Identity]int  // outbound like count per followed author (only authors with >0)
	Replies map[core.Identity]int  // outbound reply count per followed author (only authors with >0)
	Recip   map[core.Identity]bool // reciprocal follows: they follow me back
	Pins    map[core.Identity]bool // user-pinned follows
}

// Weight is the weight of one followed identity.
type Weight struct {
	ID     core.Identity
	Weight float64
}

// Weights returns the sync-priority weight for every followed author in
// in.Follows. A follow with no signal of any kind gets coldStartWeight; any
// signal (even a single like, a reciprocal edge, or a pin) lifts a follow to
// its weighted sum.
func Weights(in Inputs) map[core.Identity]float64 {
	out := make(map[core.Identity]float64, len(in.Follows))
	for _, id := range in.Follows {
		likes := in.Likes[id]
		replies := in.Replies[id]
		recip := in.Recip[id]
		pinned := in.Pins[id]
		if likes == 0 && replies == 0 && !recip && !pinned {
			out[id] = coldStartWeight
			continue
		}
		out[id] = wLikes*normLog(likes, likeSoftCap) +
			wReplies*normLog(replies, replySoftCap) +
			wRecip*boolToFloat(recip) +
			wPin*boolToFloat(pinned)
	}
	return out
}

// Sorted returns the weights in descending priority order, highest first,
// with the identity string as a deterministic tiebreak so the order of
// equal-weight follows is stable.
func Sorted(in Inputs) []Weight {
	w := Weights(in)
	out := make([]Weight, 0, len(w))
	for id, v := range w {
		out = append(out, Weight{ID: id, Weight: v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Weight != out[j].Weight {
			return out[i].Weight > out[j].Weight
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// normLog is the log(1+x) normalization with a saturating soft cap. Counts
// below the cap map to log(1+x)/log(1+cap) in (0, 1); counts at or above the
// cap saturate to 1.0. This spreads the heavy-tailed 1-vs-5-vs-20 distinction
// that linear scaling collapses (the p50 is 1 for both metrics).
func normLog(count, softCap int) float64 {
	if count <= 0 {
		return 0
	}
	if count >= softCap {
		return 1
	}
	return math.Log1p(float64(count)) / math.Log1p(float64(softCap))
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
