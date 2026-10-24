# Dead-Zen State Plan

## Problem

The network accumulates state from zens that no longer exist. Two
accumulations grow without bound:

1. **Follower reads.** The follower set is derived at read time by scanning
   every stored remote event (`ReceivedFollowers`). Every dead follower makes
   every snapshot slower. Cost per lookup grows with all stored events, not
   with the follower count.
2. **Discovered tokens.** Every zen exchange adds tokens to `knownTokens`
   with no validation, bound, or removal (`learnZenRef`). The whole set is
   offered to every peer on every session (`knownRefsList` feeding the zen
   exchange), so one bloated zen spreads to all. The set is in-memory only
   today and is lost on restart; the persisted LRU below also makes
   discovered tokens survive restarts, which they do not today.

Follows themselves are legitimate growth. A popular account must accumulate
followers, and about 200 bytes per edge is the honest price of being
followed. Nothing in this plan caps followers.

## Decisions

1. **Follower map.** One record per follower author, stored in the remote
   bucket, derived from Follow events by a rebuild that scans from zero,
   never from the sync cursor. Reads use the map: both the follower-set
   read (`ReceivedFollowers`) and the wire-side request
   (`ReceivedFollowEvents`, served per inbound crawl request), so a map
   record carries the follower's event ID, not just presence. No eviction,
   no cap.

   The map is maintained incrementally inside the `PutRemoteEvent`
   transaction: applying a Follow inserts or refreshes the record, an
   Unfollow removes it. A rebuild from zero runs at store open, lazily on
   the first follower read, so a session that never reads followers never
   pays the scan. The lanes sync continuously, so a per-sync rebuild would
   re-pay the full O(all stored events) scan every session, which is the
   cost this plan exists to remove.

   The map is derived by replaying Follow and Unfollow in timestamp +
   sequence order, like `FollowSet.Apply`: a follower who later unfollows
   leaves the set. Matching only Follow events would keep an unfollowed
   follower forever.

2. **Cursor only for posts.** The pull cursor applies to PostLog only.
   ProfileLog pulls always request from zero: the tidy ProfileLog is small,
   and pulling it from zero re-derives the follower map, so a zen that
   returns after being wiped heals in one session. With no ProfileLog
   cursor, deleting follower state can never strand an edge: the cursor
   trap does not exist.

   Steady-state cost: a from-zero ProfileLog pull is O(the peer's current
   follow set) per session both ways, so a maxed 5000-follow peer is about
   1 MB and a few hundred ms of signature verification per session. The
   tidy log keeps this the honest upper bound.

3. **Token LRU.** Discovered tokens get a persisted LRU with the same cap
   as the crawl cache (10000, a source constant). Keyed on last mention,
   refreshed on successful sync. Never evict routing-bound or bootstrap
   tokens. Tokens are offered on every session, so evicted tokens are
   relearned on the next exchange with a holder that still has them hot:
   eviction is self-healing while at least one holder mentions the token.
   The daemon's in-memory `knownTokens` set becomes a view over the
   persisted LRU, and rehydrate reads it at open, which is what makes
   discovered tokens survive restarts.

   The store already implements this LRU shape for the crawl cache
   (internal/store/crawllru.go): a forward bucket keyed by 8-byte
   big-endian touch time plus the id suffix (sorted iteration yields the
   coldest first), a reverse bucket mapping id to touch time (O(1) touch,
   set membership, and the cap count), and an evict-to-cap loop that
   skips and re-hots kept entries under a local counter, since
   `Stats().KeyN` is stale inside a write transaction. Extract that into
   one generic bbolt LRU (touch, contains, count, evict-to-cap with a
   keep predicate and an on-evict hook, reset), tx-scoped so callers can
   compose it into larger transactions, specialized twice: the crawl
   cache keeps followed identities and deletes the author's remote
   bucket on eviction; the token cache keeps routing-bound and bootstrap
   tokens and has no eviction side effect, since a token has no payload:
   the LRU entry pair is the token.

4. **Follower map cap.** Capped at `1 << 33` records (8,589,934,592), the
   smallest power of two above world population (~8.2 billion). No
   legitimate node reaches the cap, so the map is unbounded in practice.
   An over-cap Follow event is dropped at ingest without flagging: the
   author side cannot enforce in-degree, so exceeding it is not a signed
   protocol violation.

   Map lifetime and crawl eviction: a non-followed follower's author bucket,
   including the Follow evidence bytes, is deletable by crawl LRU eviction
   (`EvictCrawl`). The map record lives in the follower's author bucket, so
   the eviction deletes map and evidence together and the map can never
   claim a follower whose proof has been deleted.

5. **Identity minting.** A Follow event must carry proof of work: a
   `Nonce` field on `Follow`, covered by the signature, so the work is
   bound to author and target. The difficulty is a zero-bit count on
   BLAKE3-256 over the author pubkey, target pubkey, and nonce; grinding
   one nonce cannot amortize across targets or across a botnet's keypairs.
   The recipe is a source constant. Difficulty targets roughly 100 ms of
   grinding on a reference laptop CPU: a human follow pays nothing
   noticeable, a 5000-follow migration pays about 8 minutes once. The
   zero-bit count is fixed by the plan's benchmark rather than by
   arithmetic estimate. It is friction, not a hard barrier: optimized
   hardware grinds faster, and the cap (decision 4) is the real bound.

   The canonical Follow grows by the nonce field (~73 bytes against the
   75-byte `CapFollow`), so the cap rises to fit.

   Enforcement: the author's client refuses to sign a Follow without
   valid work. The receiver drops a workless Follow and flags its
   author, same as an oversized event. The ProfileLog rate-limit
   exemption stays: the work prices each Follow.

   Minting remains possible but no longer free. Each minted follower
   still costs one keypair, one session with the target, and now the
   priced difficulty. The exchange is linear on both sides, no
   amplification, and the cap (decision 4) makes the total finite.

## Implementation plan

Ordered so each step is independently verifiable against the code it
touches. Each step cites the decision it implements and the test or
benchmark that verifies it.

1. **Follow work.** Add the `Nonce` field on `Follow` and the grinding
   recipe as a source constant: BLAKE3-256 over the author pubkey,
   target pubkey, and nonce, requiring a zero-bit count on the digest.
   Write the grind as a function next to `Follow` in core; the author's
   client grinds a nonce before signing. In `PutRemoteEvent` and
   `PutCrawledProfile`, verify the work and drop and flag a workless
   Follow, same as an oversized event. Backup import does not re-check
   work. Raise `CapFollow` to fit the nonce field.

2. **Benchmark the grind, then pin the difficulty.** Measure the
   implementation from step 1 on the pin machine (a benchmark that
   grinds one nonce per follow over a spread of author and target
   keys), and set the difficulty constant from the measurement,
   targeting roughly 100 ms per follow. The doc does not pre-commit a
   bit count; the benchmark picks the bit count that hits the target.
   Covered by the test: a nonce ground for one (author, target) pair
   does not verify for a different target.

3. **Follower map.** Store one record per follower author per
   decision 1, maintained in the `PutRemoteEvent` transaction and in
   `PutCrawledProfile` (crawled Follow events targeting this zen count
   as observations, closing the gap from the review). Initial state
   comes from the lazy rebuild on the first follower read; both reads
   (`ReceivedFollowers`, `ReceivedFollowEvents`) and the wire-side
   request consult the map, and every write path maintains it
   consistently with `FollowSet.Apply` in timestamp + sequence order.
   The cap from decision 4 compares against the follower count; the
   drop-without-flag behavior comes out of the ingest transaction.

4. **ProfileLog cursor removal.** In the session, stop reading and
   stop persisting the per-zen cursor for ProfileLog pulls, per
   decision 2. PostLog pulls keep the cursor. Covered by the test: a
   ProfileLog pull requests from zero even when a cursor exists.

5. **Generic LRU.** Extract the crawl LRU implementation from
   `internal/store/crawllru.go` into one generic bbolt LRU: touch,
   contains, count, evict-to-cap with a keep predicate and an on-evict
   hook, reset; tx-scoped so callers compose it into larger
   transactions, per decision 3. Re-specialize the crawl cache on it,
   with the existing behavior identical (the follow flag skip, the
   bucket delete on eviction, the counter local to the transaction).
   Covered by existing crawl tests staying green plus the LRU-reuse
   test naming the shared implementation.

6. **Token LRU.** Specialize the generic LRU a second time into the
   token cache, per decision 3, keyed on last mention and refreshed on
   each successful sync. The keep predicate holds tokens with routing
   bindings and tokens loaded from the bootstrap file. The daemon's
   in-memory `knownTokens` map becomes a view over the persisted LRU:
   zen offers enumerate the LRU, and rehydrate reads it at open. The
   token cache has no on-evict side effect.

7. **Work on the wire.** Pair the author-side enforcement with the
   receiver-side check so a workless Follow never enters the store via
   any path. Keep the ProfileLog rate-limit exemption for Follow.
   Sweep every caller that signs a Follow (`SignAndAppend`,
   `store.Follow`, `store.Unfollow`, and any test helpers) to confirm
   the refusal to sign a workless Follow cannot be bypassed.

8. **Tests, then race.** Land the tests as each step lands, not in a
   final pass: each decision's tests cite the step they cover. The
   token-cache tests from the review (overfill evicts the coldest,
   keep predicates survive pressure, rehydrate and zen offers never
   resurrect evicted tokens) belong to step 6. Run the full suite under
   `-race` before merge.

## Tests

- Follower map: rebuild output equals the current follower set from a full
  ordered replay over randomized stored Follow and Unfollow events; lookup
  time is proportional to follower count at large input (regression test
  with a large fixture); advancing a cursor past a Follow event does not
  affect the map; an Unfollow removes the follower.
- Follower map cap (against a small overridden cap): the next unique
  follower's Follow event is dropped, existing records are kept, and the
  author is not flagged.
- Map lifetime: crawl eviction of a non-followed follower's bucket removes
  the map record, so no read later claims the follower.
- Cursor: a ProfileLog pull requests from zero even when a cursor exists;
  a PostLog pull still uses the cursor.
- Follow work: `Sign` rejects a Follow without a nonce meeting the
  difficulty; `PutRemoteEvent` drops and flags a workless Follow; a nonce
  meeting the difficulty for (author, target) does not meet it for a
  different target at the hash level, not merely via signature
  verification; backup import does not re-check work.
- Token LRU: overfill evicts the coldest entries; routing-bound and
  bootstrap tokens survive any pressure; rehydrate and zen offers never
  resurrect evicted tokens.
- LRU reuse: the crawl cache and the token cache run on the same generic
  implementation; touch, count, and evict-to-cap behave identically in
  both, and only the keep predicate and evict side effect differ.
