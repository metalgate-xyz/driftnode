# Security notes

This file tracks known gaps in the current implementation against the design
in `serverless-social-network-design.md`, with an explicit decision for each:
fix, accept with rationale, or defer. It is not a threat model.

## 1. PostLog confidentiality vs. the design doc

Status: documentation mismatch, not a vulnerability.

The design doc (§7.1) describes the PostLog as "synced only by zens who
follow this identity." The current implementation serves the local user's
own PostLog to any connected zen (see `fetchOwnEvents` in
`internal/sync/session.go`, reached from `Server.handleRequest`). There are no
private accounts in this phase; all logs are public to any token holder. This
is an intentional simplification for Phase 0.

Action: correct the design doc to state that PostLogs are public to any
authenticated zen in v1, and note that follower-only PostLog is a future
scope item requiring a separate access-control layer.

## 2. Zen-token trust model

Status: accepted for v1; revocation deferred.

The tailcat address token (`tc1q...`) is a public, bearer routing descriptor.
Anyone who has seen it can dial the node. There is no revocation mechanism:
once a token is out, it is valid for the lifetime of that key. Tokens
propagate through zen exchange (`MsgZens`), bootstrap files, and direct
sharing, with no way to recall one.

The tailcat listener restricts inbound connections to a single application
port (`SyncPort = 7421`); see `OnTCP` and `ServedTCPPorts` in
`internal/net/transport.go`. A token holder cannot use the tunnel as a
general VPN into the host. This bounds the blast radius of a leaked token to
the sync protocol, but it does not authenticate *which driftnode identity* is
on the other end.

Action: document the bearer-token lifetime model. Token rotation via a new
tailcat keypair is the only revocation path today; a first-class revocation
mechanism is deferred.

## 3. Auto-dial of discovered zens

Status: fixed. The auto-dial set is the follow graph.

A zen connection and a follow are the same gesture: the only reason to dial
anyone is that you follow them. `follow <token-or-pubkey>` is the one add
gesture: given a token, it dials, learns the zen's identity from the
handshake (#4), writes a Follow event, and binds the token to that identity
in the routing table; given a pubkey (offline path), it writes the Follow
event only. `unfollow` is the single remove gesture: it writes an Unfollow
event, deletes the routing binding, and drops the zen. There is no separate
`zens add` command.

`syncAllZens` reads `FollowedIdentities()` from the Profile log and dials
each identity's bound token. A followed identity with no bound token
(followed offline by pubkey) is skipped until a token is learned. Tokens
learned via zen exchange (`learnZenToken`) are recorded for discovery and
zen-exchange offers but never auto-dialed; dialing them requires a follow.

Bootstrap seeds are auto-followed on first dial (`dialBootstrapSeeds`): each
seed is dialed, its identity learned from the handshake, a Follow event
written, and the token bound. Seeds enter the follow graph like any other
zen, not a separate tier.

The `seedZens` set from the interim fix is removed; the follow graph plus
the routing table replace it.

## 4. Session-level authentication

Status: implemented (Option A: Ed25519 challenge-response). Option B
(go-libp2p Noise) remains the upgrade path when go-libp2p is introduced for
the DHT.

The Ed25519 identity key now authenticates the session, not just events. A
challenge-response handshake runs before any sync traffic: each side sends
its `driftnode:<pubkey>` identity and a fresh nonce, signs the other's
nonce, and verifies the signature against the claimed pubkey. Two new
message kinds (`MsgHello`, `MsgAuth`) in `internal/sync/sync.go`; the
handshake logic is `Session.Handshake` in `internal/sync/session.go`, called
by `RunInitiator` and `RunListener`. No new crypto dependency:
`crypto/ed25519` is already used for event signing.

The session is now bound to a driftnode identity; the tailcat token is only
how the zen was reached. The daemon's `runSession`
(`internal/daemon/daemon.go`) requires an unlocked signing key on both
outbound and inbound sessions: a locked daemon rejects inbound sessions
rather than serving unauthenticated sync. Seed nodes run always-unlocked and
are unaffected. Send and receive in the handshake run concurrently so it
works over synchronous, unbuffered connections like `net.Pipe`.

The authenticated zen identity is surfaced via `Session.SetAuthed`, so the
daemon can bind authorization policy (the auto-dial set in finding #3, and
the follow graph) to identity rather than to bearer tokens.

### Upgrade path: Option B (go-libp2p Noise)

Introduce `go-libp2p` and run the sync protocol over a Noise-secured stream.
The existing Ed25519 identity key becomes the libp2p zen ID; Noise
authenticates it during connection setup and adds forward secrecy on the
session. The cleanest integration is the custom transport the design doc
(§5.1) flags as unsettled: wrap a tailcat-established tunnel as a libp2p
transport, unifying DHT control traffic and event-log sync under one
security layer. Justified only when go-libp2p lands for the DHT, at which
point the cost is shared and Noise comes with it.

## 5. Resource exhaustion (to implement)

No per-zen rate limiting, no connection cap, no total inbound byte budget.
`maxFrameSize` is 16 MiB (`internal/sync/sync.go`); a single zen can send
many maximum-size frames. DoS is trivial for any token holder.

Action: implement per-zen rate limiting, a connection cap, and a smaller
max frame size. This is a standard hardening item, not a design gap.

## 6. Timestamp and sequence validation at merge

Status: acknowledged design property.

`mergeEvent` / `PutFollowedEvent` dedup by event ID but do not enforce
monotonic sequence or timestamp sanity (`internal/core/event.go` validates
only kind/log consistency). Profiles are last-write-wins by timestamp, so an
attacker can backdate or postdate validly-signed `Profile`/`Follow` events
to manipulate the crawl graph and search ranking. This is the Sybil problem
the design doc acknowledges; full resistance is out of scope for v1.

Action: reject implausible timestamps (e.g. future-dated beyond a skew
tolerance) at merge as a cheap, local mitigation. Full Sybil resistance is a
known, documented non-goal for v1.

## 7. Spam mitigation not implemented

Status: deferred, documented in design (§10).

The design mentions proof-of-work or rate-limited signing as a "speed bump"
on the gossip layer. It is not implemented. The crawler's in-degree metric is
Sybil-gameable; this is acknowledged.

Action: no action for v1 beyond the timestamp sanity check in #6. Document
as a known limitation.
