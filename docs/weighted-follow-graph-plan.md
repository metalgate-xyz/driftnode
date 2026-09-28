# Weighted Follow Graph: Sync Priority Plan

## Problem

`syncAllZens` (daemon.go) reads `FollowGraph()` as a flat, unweighted list
and dials every followed identity with a uniform fan-out (default 8) and a
30s per-dial timeout. There is no priority: a high-latency or offline follow
holds a concurrency slot for up to 30s, and follows the user cares about get
no preferential treatment over the long tail. The result is latency, not
liveness (every follow syncs once per 60s round), but for a social feed,
latency of fresh posts from important follows is the user-visible quality
metric.

The follow graph must become a weighted graph where the weight IS the sync
priority, computed from interaction metrics.

## Metric set

| Metric | Type | State | Computable from |
|---|---|---|---|
| Outbound likes | interaction | done | my PostLog `Like.target_id` -> author |
| Outbound replies | interaction | done | my PostLog `Post.parent_id` -> author |
| Pins | explicit | missing entirely | new local state |
| Reciprocal follow | graph signal | available now | crawled profile logs |

Trivial graph signals ("I follow them", "when I followed them") are noise
once you're in the graph and are excluded.

## Scope decisions

- Replies are NOT worth building as a feature: the threaded conversation view
  needs a global index of all replies to a post, which doesn't exist without a
  server (the crawler fetches only Profile logs by design; nobody can write
  into someone else's log; the DHT is for routing records, not one-to-many
  content indexing). But reply as an interaction metric stays: "I replied to
  author X's post" is resolvable from my own PostLog via `Post.parent_id`.
- Cold-start (no interaction history) is a false problem. Losing the db is
  fatal by design with no recovery path (design doc §8), so the only regime
  is "you have your db, hence your interaction history."
- Pins are local-only state: never synced, never signed. Likes and replies
  are signed events in the user's own PostLog.
- The weight formula is designed ONLY after all metrics exist, so
  normalization and default coefficients are informed by real metric
  distributions, not chosen in a vacuum.

## Weight formula (form settled, specifics deferred to phase 4)

Weighted sum of normalized metrics with user-configurable coefficients:

    weight(author) = w_likes   * norm( outboundLikes(author) )
                  + w_replies  * norm( outboundReplies(author) )
                  + w_recip    * reciprocal(author)
                  + w_pin      * pin(author)

Normalization scheme (log-scaling, saturating soft cap, recency-decay) is
decided in phase 4 against real metric distributions.

## Phases

Each metric phase is a vertical slice (feature + its metric end to end),
testable independently.

1. **Replies tracking e2e.** DONE. A reply is a Post with `parent_id` set (not
   a separate event kind or struct). `driftnode post --parent <event-id>`
   creates a reply via the existing Post RPC; the feed prefixes "(reply)".
   `store.OutboundReplies()` computes per-followed-author reply counts by
   resolving `Post.parent_id` to the parent's author across held PostLogs.
   Counts only authors in the current follow graph.
2. **Likes feature and tracking e2e.** DONE. `driftnode like <post-id>`
   command + gRPC RPC + signed Like event into PostLog. Follows' likes
   displayed automatically in the feed (Like events received via synced
   PostLogs). `driftnode feed --mine` filters to own posts. On-demand like
   fetch: `FetchLikes` RPC dials connected zens with a new `MsgLikeRequest`
   sync query, requesting Like events targeting the user's own post IDs
   (best-effort, expensive, user-triggered). Metric: `store.OutboundLikes()`
   computes per-followed-author like counts from own PostLog, counting only
   authors in the current follow graph.
3. **Pins feature and tracking e2e.** Pin toggle (local state, or signed
   event if pins should survive backup restore) + `pin(author)` metric +
   weight floor for pinned follows.
4. **Weight formula.** Normalization scheme and default coefficients chosen
   against real metric distributions from phases 1-3. Config knobs
   (`w_likes`, `w_replies`, `w_recip`, `w_pin`) via `driftnode config`.
5. **Make the graph weighted.** Persisted per-follow weight recomputed each
   sync round. Graph is weighted; sync still uniform.
6. **Scheduler redesign.** Replace `syncAllZens`'s flat `FollowGraph()`
   iteration + uniform fan-out with a priority queue over the weighted
   graph: high-weight follows sync first and every round; low-weight
   follows round-robin across rounds; a dialing peer that times out stops
   blocking the next high-weight follow. Regression test: top-K high-weight
   follows sync in round 1 even when the long tail is unreachable.

## Current state of interaction primitives

`KindLike` is now fully implemented: CLI command, gRPC RPC, signed event
creation, and metric extraction. The on-demand like fetch adds a new sync
query (`MsgLikeRequest`) for requesting Like events by target ID from
connected zens.

Replies (Phase 1) are implemented as a Post with `parent_id`:
`KindReply` and the `Reply` struct have been removed. The metric
`store.OutboundReplies()` resolves reply parents to followed authors.

Remaining: Pins (Phase 3), weight formula (Phase 4), weighted graph
(Phase 5), scheduler redesign (Phase 6).
