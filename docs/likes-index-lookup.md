# LikesForPosts: serve likes through the reference index

## What changed

`store.LikesForPosts(targetIDs)` answers "which Like events held locally
target these posts?". It now reads the reference index (`bucketEventRef`)
instead of scanning and decoding every stored event. One function body
changed; no ingest, index, or storage-layout changes.

## Why

The reference index already exists. Ingest writes an entry for every
stored event that references another event (Like, Reply, Delete): key is
(target event ID + referencing event ID), value is (author, kind). The
index is maintained through cascade delete, prune, and crawl eviction, so
it stays consistent with the stored events.

The old body ignored it: per request it decoded every event in the own
PostLog and every event in every remote author bucket, keeping likes whose
target matched. The cost grew with the total store size, not with the
answer size.

The call path that makes this matter is on the wire protocol: a peer sends
MsgLikeRequest, the receiving zen answers with `handleLikeRequest`, which
calls `LikesForPosts` on its own store. Any connected peer can send the
request, and the requesting side dial-fans-out to every known zen. The cost
of the request lands on the receiver and grows with the receiver's store,
so every responder paid a full-store scan per request.

The new body reads, per requested target, the reference entries whose key
prefix is that target, keeps the Like entries, and fetches each event's
bytes with a point lookup (own PostLog bucket if the author is the local
identity, otherwise the author's remote bucket). Cost grows with the number
of likes on the requested targets, not with store size.

`bucketEventRef` is the same index cascade delete consults (`refsByTarget`),
so serving reads use a path that already has correctness obligations and
tests. Every entry's event is fetched by ID from the author's bucket before
being served, so a lookup result always corresponds to a stored event.

Correctness note: no local identity is required to serve likes. A store
with no identity can hold and serve remote likes, matching the old scan's
behavior.

## Tests

- `TestLikesForPostsIndexEquivalence`: own and remote likes in every
  storage shape (own log and remote authors, targeting own and remote
  posts), cascade-deleted and unknown and duplicate targets; asserts the
  exact expected set, both directions (nothing missing, nothing extra).
- `TestLikesForPostsAfterRemovals`: cascade delete and prune leave the
  served set empty and repeated queries stay empty. The prune fixture
  asserts the events were stored before pruning, because the retention
  window would otherwise drop them at ingest and the test would pass
  vacuously.
- Existing like tests (fetch, cascade, delete-targeting-like,
  prune-at-ingest) unchanged and green.
- `BenchmarkStoreLikesForPosts`: query targets are captured in the bench
  fixture (the liked post IDs the setup already computes), consistent with
  the other benchmarks in the file.

## Scope

Store package only. Signature and returned contract unchanged. Results are
still best-effort local observation; order is unspecified.
