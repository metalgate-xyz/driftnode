# DOS Protection Plan

## Requirements

Every event is Ed25519-signed by its author and verified on receipt
(`core.SignedEvent.Verify`). No one without the author's key can produce an
event that passes verification. But verification proves authenticity, not
volume. A peer you sync with can feed you an unbounded number of valid
signed events, and the store accepts every verified event unconditionally.
The code has no bound on size, rate, or retention, so a single peer can
exhaust your disk and memory with genuine signed content.

The bounds:

1. **Bounded per-event size.** A single event cannot be arbitrarily large.
   Every kind has a cap; the author's own client refuses to sign over it, so
   a receiver seeing an oversized event knows the author forged it.
2. **Bounded production rate.** An author cannot emit events faster than the
   network can absorb them.
3. **Bounded cumulative storage.** Stored events for one author cannot grow
   without limit over time. PostLog uses retention (reject old events at
   ingest and prune stored events that age out). ProfileLog uses compaction
   (the author keeps the log tidy at write time: one Follow per currently
   followed target, one Unfollow per previously-followed-but-now-unfollowed
   target, one Profile, keeping signed originals).
4. **Reject future events.** An author cannot stamp events far into the
   future to evade the rate limit.
5. **Linked-event existence.** A Like or Reply that targets a post the
   receiver does not hold is dropped at ingest, not stored as orphan junk.
6. **Max 5000 follows.** An author cannot sign more than 5000 net follows.
   The limit is enforced on the author side: `SignAndAppend` rejects the
   5001st Follow.
7. **Tidy ProfileLog.** The author keeps the ProfileLog tidy at write
   time: it removes superseded events on each append, so the log always
   holds only the signed events that contribute to the current state, not
   the full history.

No bound can frame an innocent author. Every check runs on events that
passed `Verify`, so a flag means the author's key signed the offending
event. The author is compromised or misbehaving, never framed.

## High level design

### Phase 1: Event size cap

Every event kind has a cap on the canonical `Event` bytes (the signed
payload the signature covers, not the on-wire `SignedEvent`). Fixed events
have an exact max; variable events have a max. The author side refuses to
sign an event over its cap, so a legitimate author can never produce one.
The receiver side checks the size after `Verify` passes: a valid signature
on an oversized event means the author's key signed something their own
client would have blocked, so the author is flagged and the event is
dropped. The check runs after `Verify`, not inside it, so the receiver
can tell a bad signature or transit tamper (drop, no flag) from an
oversized event (flag and drop). This keeps the no-framing invariant: a
flag always means the author's key signed the offending event. The CLI
and TUI bound user input before composing an event, so the user gets
immediate feedback instead of a refusal at sign time.

| Event | Log | Cap | Payload |
|---|---|---|---|
| Post | PostLog | 5120 B | `Text` + `MediaHashes[]` + `ParentID` share the budget |
| Like | PostLog | 72 B | Fixed: `TargetID` (32 B) |
| Delete | PostLog | 72 B | Fixed: `TargetID` (32 B) |
| Profile | ProfileLog | 1024 B | `DisplayName` + `AvatarHash` |
| Follow | ProfileLog | 75 B | Fixed: `TargetPubkey` (32 B) |
| Unfollow | ProfileLog | 75 B | Fixed: `TargetPubkey` (32 B) |
| Detail | DetailLog | 2048 B | `Bio` + `FirstName` + `LastName` + `Location` |

Sizing. Every `Event` is a 5-item CBOR map: 4 fixed fields (Kind, Log,
Timestamp, Sequence) plus one payload. The fixed header is 33 B for
PostLog, 36 B for ProfileLog, 35 B for DetailLog (map header + the four
fields, worst-case full-width timestamp and sequence). A 32-byte field
(EventID, pubkey, ContentHash) encodes as 34 B. So Like and Delete are
33 + 2(key) + 1(map) + 2(key) + 34 = 72 B; Follow and Unfollow are
36 + 2 + 1 + 2 + 34 = 75 B. The variable caps leave room for the
largest legitimate payload: a 5 KB Post holds ~4000 B of text plus 30
media hashes and a parent; a 1 KB Profile holds a ~900 B display name; a
2 KB Detail holds a paragraph bio plus the short name and location fields.

### Phase 2: Rate limiting

No two events of the same kind from the same author in the same second,
enforced on both sides. The author side (`SignAndAppend`) knows its own
wall clock: it holds a sub-second event until the next second. The receiver
side (`PutRemoteEvent`) only has the signed `Event.Timestamp`: it compares
the event's second to the last second seen for that author and kind, and
rejects a same-second event and flags the author. Wall-clock drift between
the two sides is bounded by the future-timestamp check (phase 4), which
rejects events more than 60s ahead of the receiver clock. An author who
exceeds 1/sec for any one kind produces two same-second events of that kind,
is flagged, and has sync stopped. The bound is per kind: an author can
emit 1 Post, 1 Like, and 1 Delete per second, three independent operations,
without flagging. PostLog events (Post, Like, Delete) are rate-limited at
1/sec per kind. ProfileLog events (Follow, Unfollow, Profile) are not
rate-limited, because bulk-follow during onboarding is a legitimate burst.
ProfileLog volume is bounded instead by compaction (phase 7): the author
keeps the log tidy at write time, so a burst of Follow events removes the
superseded ones and costs only the durable remainder.

### Phase 3: Retention

PostLog events older than a per-node retention window are dropped at ingest
before they are written, and stored events that age past the window are
pruned. This bounds cumulative storage: an author rate-limited at 1 event
per second per kind, across the three PostLog kinds, can store at most
three events per second times the retention window. ProfileLog does not use
retention; it uses compaction (phase 7), because the follow set is a
projection over Follow/Unfollow events and dropping old events by
timestamp would corrupt the current state.

### Phase 4: Reject future events

Reject events whose timestamp is ahead of the receiver clock by more than
60s. Without this, an attacker can stamp future timestamps 1 second apart
to evade the rate limit. Dropped events sync normally once the wall
clock catches up, so an author with a fast clock is not penalized. The
author sets its own wall clock at signing time, so future skew only
arises from a fast clock or deliberate forging.

### Phase 5: Linked-event existence

A Like is stored only if its `TargetID` resolves to a non-tombstoned post
the receiver already holds. A Reply (a Post with `ParentID` set) is stored
only if the parent post exists locally. Likes and replies targeting
deleted or unknown posts are dropped at ingest. When a Delete arrives for
a post that has stored likes, those likes are dropped too, so the
projection stays clean. The same logic covers any event that references
another: a Delete targets any event, a Like targets a post, a Reply
targets a post. A user following Bob but not Alice will not see Bob's
likes on Alice's posts, because the receiver never synced Alice's PostLog,
so the target ID is not in the index.

The existence gate is receiver-only. Linked-event existence is a
receiver-local property, not a universal invariant: a Like's target may
exist for its author and be absent from a given receiver, because the
receiver does not sync every author's PostLog. The receiver drops an
orphan without flagging the author; it is a local utility filter, not a
protocol violation. The own path (`AppendOwnEvent`) is not gated: it is
a pure set-union write, so a backup restore re-adds a user's likes on
other authors' posts even though those posts (in the remote bucket) are
not part of the backup. Own orphan volume is bounded by the rate limit
(phase 2), and the projection tolerates the orphans that remain. The
author side prevents orphans from being signed: the CLI and TUI offer
Like and Reply only against posts the local node holds, so a legitimate
author never signs an orphan in the first place.

### Phase 6: Follow cap

An author cannot sign more than 5000 net follows. The limit is enforced
on the author side: `SignAndAppend` rejects the 5001st Follow. The cap
keeps the crawl cache bounded and matches the Twitter platform cap and the
sizing in the weighted-follow-graph plan.

The cap is enforced on the author side in `SignAndAppend`: you cannot
sign a Follow that would push your net follow count past 5000. Because
the author's own client blocks the 5001st follow, a ProfileLog with more
than 5000 net follows is a protocol violation. Every Follow event is
signed by the author's key, so the author is not framed. The receiver
flags the author and stops syncing their ProfileLog.

Storage cost per author at the cap:

| Net follows | Per author | 10000 authors |
|---|---|---|
| 150 (Dunbar) | ~30 KB | ~300 MB |
| 5000 (cap) | ~1 MB | ~10 GB |

Each compacted Follow event is ~200 bytes: the author's 62-byte identity,
the 64-byte signature, the 32-byte target pubkey, the Event envelope, and
CBOR framing.

### Phase 7: Tidy ProfileLog

The author keeps the ProfileLog tidy at write time, so the stored log
always holds only the signed events that contribute to the current state.
A Follow for a new target is appended; a Follow for an already-followed
target supersedes the previous Follow, which is removed; an Unfollow for a
followed target removes the matching Follow, and any prior Unfollow for that
target, and is kept (it is the convergence tombstone for peers that synced
the old Follow); a Follow for a not-currently-followed target also removes
any prior Unfollow for that target, so a follow/unfollow cycle leaves one
tombstone, not one per cycle; a Profile supersedes the previous one, which
is removed. The tidy log holds one Follow per currently-followed target,
one Unfollow per previously-followed-but-now-unfollowed target, and one
Profile. Every kept event still verifies. Mutations are rare and serving is
frequent (every sync round and every LRU re-crawl reads the log), so the
work of removing a superseded event belongs at the rare mutation, not at
the frequent read. A receiver that syncs a tidy log stores it directly: no
dead events to filter, no cold-start replay, no per-receiver compacted
state.

The Unfollow tombstones are the irreducible cost of convergence with
unknown peer cursors in an append-only-plus-projection model. A peer that
synced the old Follow before the author compacted it still holds its copy;
the Unfollow is how that peer learns the edge is gone. New peers never
saw the Follow, so the Unfollow is a no-op for them. Removing a stale
tombstone when the target is re-followed is safe: a peer whose cursor
predates the removed tombstone re-requests from there, and the skew-window
re-request plus set-union dedup delivers the newer Follow or the newer
Unfollow, whichever is current. The tombstones are bounded (one per
target ever unfollowed, ~200 B each) and never grow the log past one event
per target per edge state.

ProfileLog Delete events are not supported. Unfollow already covers the
Follow case (and converges with peers via the tombstone), and a new Profile
covers the Profile case. A ProfileLog Delete that removes a Follow without
leaving a tombstone would break convergence: peers holding the old Follow
would never learn the edge is gone. `validateEvent` rejects `KindDelete`
in `ProfileLog` so no author can sign one.

## Low level design

### Phase 1: Event size cap

Add per-kind size caps in `core` (see the table in the high-level section).
Author side: `Sign` already computes the canonical bytes before signing;
reject an event over its cap there. Since `SignAndAppend` is the only
caller, no legitimate author can produce an oversized event. Receiver
side: in the ingest path, after `Verify` passes, compare the canonical
bytes against the cap; a valid signature plus an oversized payload means
the author's key signed something their own client would have blocked,
so flag the author and drop. The check runs after `Verify`, not inside
it: a bad signature or transit tamper is a drop without a flag, while an
oversized event is a flag and drop. Both checks use the same canonical
bytes `Verify` computes for the signature.

### Phase 2: Rate limiting

`SignAndAppend` rejects an event of the same kind in the same second (by
the author's wall clock) as the previous event in that log. For the author
side, the choice between a busy-wait and a bump to the next second is part
of the implementation. A bounded queue (single digits, e.g. 4) holds
sub-second events; overflow rejects the incoming event, not the queued
ones. A transient status line in the TUI ("holding, posting in <1s...")
and a stderr message in the CLI notify the user only when a hold happens.
`PutRemoteEvent` rejects an event whose signed `Timestamp` is in the same
second as the last event of the same kind from the same author. The
receiver stores `lastEventSecond` per author per kind in a new bbolt
bucket (not `bucketFollowState`, which is per-identity and has no
per-kind slot): a `cursor`-style bucket keyed by author + kind to a
1-second-granularity int64. On a same-second violation for a PostLog
event from a followed author, set `flagged` in `bucketFollowState` (a
third byte on the existing two-byte `followed`/`pinned` record) and stop
syncing that author's PostLog. ProfileLog events are exempt: bulk-follow
onboarding is legitimate, and the net-follow cap (phase 6) is the bound
that applies.

### Phase 3: Retention

The retention window is a single knob read from an env var for now; the
full `driftnode config` system (design doc §12.2) is out of scope for this
plan. In `PutRemoteEvent`, reject a PostLog event whose timestamp is older
than `now - retention` before the bucket write. In `pullLog`, raise the
`after` cursor to `max(last_cursor, now - retention)` so the peer does
not send old events the receiver would drop. Pruning removes stored
remote PostLog events whose timestamp is older than the window: it runs
at daemon shutdown and on user demand, scanning the `crawl` bucket's
per-author sub-buckets. Retention bounds the `crawl` bucket only; the
author's own PostLog (`bucketOwn`) is retained in full, because own logs
are backup-critical.

### Phase 4: Reject future events

Add `maxFutureSkew` (60s) in `core`. In `PutRemoteEvent`, reject an event
whose `Timestamp > receiverNow + maxFutureSkew`. The check reads the
receiver's wall clock, unlike the same-second rule which reads only signed
timestamps, so it does not set `flagged`: a fast clock is skew, not
misconduct. The event is dropped silently and resyncs when the clocks
converge. The check belongs on the receiver path, not in `validateEvent`:
`validateEvent` takes only the `Event` struct (no wall clock) and is also
called by `Sign` at authoring time, where the timestamp was just set to
`Now64()` and future skew is meaningless.

### Phase 5: Linked-event existence

Add two persisted bbolt buckets, both maintained at ingest, delete, and
prune time.

The existence index (`bucketEventIndex`) maps EventID to the event's
author and kind. It covers every stored PostLog event (own + remote),
so it answers in O(1): does this event exist, and is it tombstoned? A
Like's `TargetID` and a Reply's `ParentID` both look up here. On event
ingest: add. On Delete: remove (the bytes stay in the bucket, but the
absent index entry makes any later reference drop). On prune: remove.
The bytes must stay because sync is append-only: a peer re-sends events
past the cursor, and the receiver dedups by event ID (set union).
Deleting the bytes would break dedup, so the next sync round would
re-store and re-index the deleted event, silently undoing the delete.
The index entry removal is the tombstone; the bytes are the dedup
guard. The index covers PostLog events only: the existence gate is a
PostLog concern (Like targets a post, Reply targets a post, Delete
targets a post or a like). ProfileLog events are not indexed;
ProfileLog Deletes are handled by tidy-at-write (phase 7), not by the
existence gate. The crawler stores only ProfileLog events via
`PutCrawledProfile`, which does not touch the index.

The reference index (`bucketEventRef`) is a composite key of
`TargetEventID + SourceEventID` mapping to the source event's author and
kind. It covers every referencing PostLog event (Like, Reply, Delete).
A range scan by `TargetEventID` prefix finds every event referencing a
target in O(k). On ingest of a referencing event: add (after the
existence index confirms the target exists). On Delete of a target:
range-scan by the deleted ID, drop every referencing event from the
store, then remove their index entries. The cascade runs before the
Delete's own ref entry is added, so the range scan does not find the
Delete itself. On prune: same.

The store has no such index today: `LikesForPosts` linear-scans every
held bucket, and the weight-gathering code rebuilds an in-memory
event-ID-to-author map per call. Both become O(1) or O(k) lookups against
the persisted index. The index is an ingest accelerator; the projection
(`Tombstoned`) still works from the stored Delete event.

The existence gate runs only in `PutRemoteEvent`. `AppendOwnEvent` is a
pure set-union write: it maintains the indexes (an ingested own event is
added to `bucketEventIndex`, and a referencing own event to
`bucketEventRef`) but does not look up the target. Existence is
receiver-local, so the own path does not gate.

The author side keeps orphans from being signed as a UX guard. The CLI
and TUI offer Like and Reply only against posts the local node holds.
The post ID comes from the feed or a held-post listing, never free-form
entry of an unverified ID. A Reply's `--parent` is resolved against the
local PostLog before the event is composed; a Like targets a post ID the
feed already rendered from held events. A post ID the local node does
not hold is rejected before signing with a "post not found locally"
error. This mirrors phase 1: the author's own client bounds input
before composing, the user gets immediate feedback, and a signed orphan
is never produced.

### Phase 6: Follow cap

On the author side, `SignAndAppend` rejects a `KindFollow` that would
push the net follow count past 5000. On the receiver side, in
`PutRemoteEvent` and `PutCrawledProfile`, after projecting the author's
ProfileLog, if the net `FollowSet` exceeds 5000, set `flagged` in
`bucketFollowState` and skip the store: the author's own client would
have blocked the 5001st follow, so this is a protocol violation by a
signed key.

The crawl cache is a bbolt-backed LRU. Today the crawler's in-memory
`visited` map bounds to `maxCache` but never evicts, and the on-disk
`bucketCrawl` grows without bound. Replace the `visited` map with a
persisted LRU bucket (`bucketCrawlLRU`) keyed by last-touched timestamp
(int64 nanoseconds) + author Identity, so bbolt's sorted key order puts
the coldest author first. Touch (on crawl or sync): delete the old
timestamp key, insert the new one. Evict (when the on-disk count exceeds
`maxCache`): read the first key (the coldest), delete that author's
sub-bucket from `bucketCrawl`, delete the LRU key. O(1) amortized per
eviction, no linear scan over all authors. The composite key (timestamp
+ identity) handles collisions when two authors are touched in the same
nanosecond.

### Phase 7: Tidy ProfileLog

The author maintains a tidy own ProfileLog at write time. On each
ProfileLog append, before signing, the author removes the superseded
event from the own log: a Follow for an already-followed target removes
the previous Follow for that target; a Follow for a
not-currently-followed target removes any prior Unfollow for that
target; an Unfollow for a followed target removes the matching Follow
and any prior Unfollow for that target; a Profile removes the previous
Profile. The new event is then signed and appended. The tidy log holds
one Follow per currently-followed target, one Unfollow per
previously-followed-but-now-unfollowed target, and one Profile.

`validateEvent` rejects `KindDelete` in `ProfileLog`: Unfollow covers
the Follow case and converges with a tombstone, a new Profile covers
the Profile case, and a ProfileLog Delete that removes a Follow without
a tombstone would break convergence with peers holding the old Follow.

The lookup for the superseded event is by target pubkey (Follow,
Unfollow) or by kind (Profile), bounded by the follow cap (phase 6):
the scan covers Follows plus Unfollows plus one Profile, so at most
~10001 events, and the removal is O(n) in the follow set, not in the
history. A persisted monotonic sequence counter is required, independent
of bucket key count: `stampSignAppend` today derives `Sequence` from
`OwnEventCount` (key count), which drops when events are removed, so the
next event could get a lower or colliding sequence. The counter never
decreases.

The receiver needs no compaction. A tidy log has no dead events, so the
receiver stores what it receives directly. The projection
(`FollowSet.Apply` over Follow/Unfollow) is unchanged: it still replays
the events it holds, which for a tidy log are exactly the contributing
ones. No per-receiver compacted state, no cold-start replay.

## Security considerations

**No framing.** Every check runs on events that passed `Verify`, so a flag
means the author's key signed the offending event. No peer can produce
that. The author is compromised or misbehaving, never framed.

**Size-cap forgery.** An oversized event has a valid signature: the
author's key signed the payload. But the author's own client refuses to
sign over the per-kind cap, so a receiver seeing an oversized event knows
the author forged it. The flag is justified, and the event is dropped.

**Clock skew.** The future-timestamp check reads the receiver's wall clock,
unlike the same-second rule which reads only signed timestamps. A receiver
with a fast clock rejects valid events until the clocks converge. The
margin (60s) is wide enough to absorb realistic NTP drift. The check
rejects rather than flags, so a fast clock causes dropped events, not a
false accusation.

**Compromised author the node still follows.** If the author's key is
compromised and the node still follows them, the follow state passes. For
PostLog, their flood is rate-bounded (1/sec per kind) and retention-bounded
but not stopped. For ProfileLog, the net-follow cap bounds the follow set
and tidy-at-write bounds the history, but an attacker who stays within the
caps is not stopped. This is correct: the node opted into the follow. The
bounds limit the damage; unfollowing is the manual stop.

## Plan

### Phase 0: Renaming

Rename both `mergeEvent` methods (`Client.mergeEvent` and
`Session.mergeEvent`, which both route own-vs-remote to the right bucket)
to `routeSyncedEvent` (they route, they do not merge in a CRDT sense), and
`store.PutCrawledEvent` to `store.PutRemoteEvent` (the bucket holds both
followed PostLogs and crawled ProfileLogs, so "crawled" misdescribes half
the traffic). No behavior change.

### Phase 1: Event size cap

Tests: an oversized `Post.Text` event, an oversized `Profile.DisplayName`
event, and an oversized `Detail.Bio` event are all refused by `Sign` on
the author side and flagged-and-dropped on the receiver side after the
fix. An event at its cap is still accepted. A Like or Delete at its
fixed size (72 B) is accepted; one byte over is flagged. An oversized
event with a bad signature is dropped without flagging: `Verify` runs
first, so a transit-tampered event is a drop, not a flag.

### Phase 2: Rate limiting

Tests: `SignAndAppend` called twice in the same second produces two events
on buggy code; after the fix, the second is bumped to the next second. Two
signed events of the same kind from the same author in the same second are
both stored on buggy code; after the fix, the second is rejected and the
author is flagged. A replayed duplicate does not false-flag the author
(dedup runs before the rate check). A same-target like in the same second
returns the existing like's event ID (idempotent, no hold). A burst of
Follow events in the same second from the same author does not flag
(ProfileLog is exempt). A Post and a Like from the same author in the
same second are both stored, no flag (the check is per kind).

### Phase 3: Retention

Tests: an old PostLog event (older than the window) is stored on buggy
code; after the fix, it is rejected at ingest. The pull cursor for a follow
with a low last-cursor is raised to `now - retention`. A stored remote
event that ages past the window is pruned at shutdown or on demand. An
own PostLog event older than the window is retained, not pruned. An event
timestamped exactly at `now - retention` is kept; one at
`now - retention - 1ns` is dropped (boundary check).

### Phase 4: Reject future events

Tests: a signed event stamped 2 minutes ahead of the receiver clock is
stored on buggy code; after the fix, it is rejected, and the author is not
flagged. An event stamped 30s ahead (within `maxFutureSkew`) is stored
normally.

### Phase 5: Linked-event existence

Tests: a Like targeting a post the receiver does not hold is stored on
buggy code; after the fix, dropped. A Like targeting a tombstoned post is
dropped. A Reply whose parent post does not exist locally is dropped. A
Delete for a post with stored likes leaves the likes in place on buggy
code; after the fix, the likes are dropped on delete-receipt. A Delete
for a post with stored replies drops the replies too. The existence and
reference indexes are updated on ingest, delete, and prune: after a
prune, a Like targeting the pruned post is dropped. A Like targeting a
held post is stored; a Reply targeting a held parent is stored (positive
cases). A Like targeting the local user's own post (in `bucketOwn`, not
`bucketCrawl`) is stored: the existence index spans both buckets. A
Delete targeting a Like (not a post) removes that Like from the
existence index and the reference index (the bytes stay for dedup, but
the absent index entry makes the Like invisible to queries that check
the index, e.g. `LikesForPosts`).

### Phase 6: Follow cap

Tests: `SignAndAppend` rejects a Follow that would push the net count
past 5000 on buggy code; after the fix, the 5001st Follow is rejected.
On the receiver side, an author with 10,000 net follows (bypassed the
author-side cap) is flagged in `bucketFollowState` and the events are not
stored on buggy code; after the fix, the author is flagged and sync
stops. An author with 150 net follows is stored normally. A crawl
exceeding `maxCache` authors evicts the coldest so the on-disk count
stays at `maxCache`.

### Phase 7: Tidy ProfileLog

Tests: after a Follow for a new target, the own ProfileLog holds exactly
one Follow for that target. After a second Follow for the same target, the
first is removed and only the second remains, and it still verifies.
After an Unfollow for a followed target, the matching Follow is removed
and the Unfollow is kept. After a second Profile event, the first is
removed and only the second remains. An author who follows Alice,
unfollows Alice, then follows Alice again has exactly one Follow for
Alice (the latest) and one Unfollow for Alice (the convergence tombstone
from the first cycle), and both verify. An author who follows Alice,
unfollows Alice, follows Alice, then unfollows Alice again has exactly
one Follow and one Unfollow for Alice: the second Follow removed the
first Unfollow, and the second Unfollow removed the second Follow, so
no tombstone from the first cycle survives. An author who emits two
Profile events has exactly one Profile (the later one). A ProfileLog
Delete event is rejected by `validateEvent`. The persisted sequence
counter is monotonic: after a tidy-at-write removes an event, the next
event gets a sequence strictly greater than the last, never lower or
colliding. A tidy log served to a cold-start receiver is stored directly,
and the FollowSet and Profile projected from the stored set equal those
projected from the full history.

## Comparison to other P2P social networks

driftnode's threat model differs from its peers in one load-bearing way:
sync is direct peer-to-peer over tailcat, which is pure transport. SSB,
Nostr, and AT Protocol all have relays or PDSes that store, serve, and can
selectively filter events. In driftnode, the peer you connect to serves its
own logs; the follow graph determines who you dial, not what you request.
Tailcat cannot forge (signatures), cannot frame (signatures +
transport-only), and dropping bytes is a network failure you re-pull, not an
attack. So driftnode's DOS problem is about volume from peers you sync
with, not about malicious intermediaries.

| Concern | driftnode | SSB | Nostr | AT Protocol (Bluesky) |
|---|---|---|---|---|
| Auth model | Ed25519-signed events, no chain | Ed25519-signed, `previous` hash chain per feed | Ed25519-signed events, no chain | Signed commits in a Merkle tree (MST) per repo |
| Relay role | Pure transport (tailcat) | Peer/pub, stores and serves feeds | Stores and serves events, may filter | PDS stores repos; relay aggregates firehose |
| Per-event size cap | Per-kind: 72 B-5 KB (phase 1) | ~8KB box-stream body, 5MB blobs separate | Relay-set, commonly 10-100KB | 1 MiB per record, 2 MB per commit |
| Rate limit | 1/sec per author per kind (phase 2; up to 3/sec across PostLog kinds), receiver-enforced by signed timestamp | None protocol-level, "replicate only who you follow" | Relay-set (e.g. 10/60s), NIP-42 auth | PDS-enforced: 5000/hr per DID |
| Future timestamp | Reject if > now + 60s (phase 4) | Not addressed | Not addressed | Not addressed |
| Retention | Reject-at-ingest + local pruning (phase 3) | None, full feed replicated forever | Relay-set, often oldest-first LRU eviction | Full repo history retained |
| Follow graph | Append-only ProfileLog, author compacts at write | Append-only `contact` messages | Replaceable kind-3, latest only | Records in repo, latest wins |
| Follow cap | 5000 net follows, author-side (phase 6) | None, replication is the bound | None protocol-level | None protocol-level |
| Linked-event existence | Like and Reply dropped if target missing (phase 5) | N/A | N/A | N/A |
| Compaction | Author-side, tidy at write, keeps signed originals (phase 7) | None, full feed history | Replaceable events overwrite | Latest record wins, no history |
| Defeats relay framing | Yes (signatures + transport-only) | Yes (signatures + chain) | Yes (signatures) | Yes (signatures + MST) |
| Defeats author flood | Size cap + 1/sec-per-kind rate (flag stops sync) + retention + tidy-at-write | "Don't follow them" + manual unfollow | Relay rate limits + unfollow | PDS rate limits + unfollow |

The rows where driftnode is stricter follow from a deliberate posture:
descoping in-band media removes the single largest DOS and abuse surface
that the other three carry (SSB 5MB blobs, Nostr media via relays, AT
Protocol 1MB blobs), which is what makes a per-kind cap in the low KB
range possible at all. driftnode bounds volume at the source: ProfileLog
is tidy at the author, PostLog is bounded at the receiver by retention and
the rate limit, by construction, instead of leaving it to relay discretion
or a manual unfollow.

The rate limit is receiver-enforced from the signed timestamp, against a
threat model where the author is the adversary and the transport is pure.
SSB and Nostr cannot self-certify rate from signed data either; AT
Protocol can, via its MST `rev`, but enforces rate centrally at the PDS.
driftnode enforces it at the receiver, by construction, instead of leaving
it to relay discretion or a manual unfollow.
