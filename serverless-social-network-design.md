# Project Codename: driftnode
### A Serverless-First Social Network — Design Document v3.0

---

## 1. Goals & Non-Goals

**Goals**
- No company-owned backend: no accounts database, no auth server, no central feed server. A node in the network is called a **zen**: your own `driftnode` binary is your zen, and every other user you sync with is a zen.
- Two client artifacts, one language, no other application languages and no hand-written JavaScript: a **browser app compiled from Go to WebAssembly** and a **native zen binary** (`driftnode`, also Go) that runs identically on any machine — VPS, Raspberry Pi, laptop, desktop.
- Local-first: every user's posts and follow-graph live on their own device. The client *is* the database.
- Manual, user-controlled backup: export the account's local state as a single file; re-import it on any device to restore identity and history.
- Peer-to-peer sync, no mandatory intermediary for message delivery.

**Non-Goals / stated constraints**
- **Zero-infrastructure discovery between total strangers is not achievable.** Two zens with no prior relationship cannot find each other on the open internet without *some* rendezvous point. This is minimized to the smallest possible, swappable, community-runnable component (§6) — never a component that holds user data.
- No guaranteed real-time global delivery. This is a gossip network, not a queue; offline zens catch up when they reconnect.
- No built-in content moderation server. Moderation is client-side — mute/block lists a user subscribes to (§10).
- **No private messaging in v1.** The data model (§7) is entirely public, signed, append-only event logs designed for gossip replication. Private messages require a different primitive (pairwise key exchange, encrypted-at-rest storage) — a second system, not a variant of this one. Deferred, not silently missing.

---

## 2. High-Level Architecture

```
┌─────────────────────────── Browser (WASM, Go) ──────────────────────────────┐
│                                                                              │
│  UI layer (go-app)                                                          │
│      │                                                                      │
│  Shared core module (identity, event log, canonical encoding, signing) —    │
│  the same Go code compiled here and into driftnode (§3)                 │
│      │                                                                      │
│  ┌───────────────┐   ┌────────────────┐   ┌────────────────────────────┐   │
│  │ Identity/Keys │   │ Event Log Store │   │ Network Stack               │   │
│  │ crypto/ed25519│   │ signed events   │   │ pion/webrtc (js/wasm build) │   │
│  └───────────────┘   └────────┬────────┘   └────────────┬───────────────┘   │
│                                │                         │                   │
│                     driftnode-storage (self-owned)  dials out only          │
│                     persistent local store         (§5)                    │
│                                                                              │
│  Export/Import: single-file backup ↔ browser download/upload               │
└──────────────────────────────────────────────────────────────────────────┘
                │  event-log sync over WebRTC data channels │
                ▼                                            ▼
        Browser zen (same stack)              driftnode (native, Go, §5)
                                                same shared core module +
                                                 • pion/webrtc (native build) → browser zens
                                                 • tailcat                    → native mesh (§5.1)
                                                 • go-libp2p kad-dht          → DHT discovery only
                                                bridged onto one event-log
                                                sync protocol (§7.3)
```

Everything in this diagram is zen-owned. The only piece that isn't literally "someone's browser or someone's binary" is a thin, replaceable, signed static file (`bootstrap.yaml`, §6.1) used to find the *first* zens — never a stateful service, never a data holder.

Browsers can only dial out over WebRTC; WASM has no viable `libp2p` or Tailcat transport today. `driftnode` therefore runs two connection-establishment mechanisms — `pion/webrtc` to terminate browser connections, Tailcat to reach other native zens through NAT (§5.1) — plus `go-libp2p`'s `kad-dht` purely for DHT-based zen discovery (finding *which* zen to reach, independent of *how*). All of this is bridged at the application layer, not the transport layer: every path feeds the same event-log sync protocol (§7.3), so which mechanism delivered a given piece of gossip never leaks into the data model. Because both the browser and native builds compile the same shared Go module (§3), that sync protocol is one implementation, not two that need to agree.

---

## 3. Client Platforms

One language, one shared core, two build targets:

- **Shared core module** — plain Go, no platform-specific dependencies: identity and keypairs, the event/log data model, canonical CBOR encoding, and signing/verification (§7). This is compiled unmodified into both artifacts below. There is no cross-language wire-spec-matching risk to manage, because there is no second implementation — the same code either produces a valid signed event or it doesn't, on either target.
- **Browser app** — the shared core plus a WASM-only layer: **go-app** for the UI (a declarative, component-based Go framework purpose-built for compiling to WebAssembly), **`pion/webrtc`** compiled with its `js,wasm` build tag (in this mode it becomes a thin wrapper over the browser's own native `RTCPeerConnection`/`RTCDataChannel` via `syscall/js`, rather than a separate implementation), and a small, self-owned `syscall/js` wrapper around IndexedDB for persistence (§4). Built with the standard Go toolchain (`GOOS=js GOARCH=wasm`); the only JavaScript involved anywhere is `wasm_exec.js`, the small runtime-loader shim the Go toolchain itself generates to bootstrap the WASM module — not application logic, and not something this project authors.
- **`driftnode`** — the shared core plus native-only packages: `tailcat` (direct import), `go-libp2p`'s `kad-dht`, `pion/webrtc` compiled natively (a full WebRTC implementation, since there's no browser to lean on), `bbolt` for storage, and `cobra`/`bubbletea` for the CLI/TUI (§12).

**One implementation, not two.** There is no cross-language wire-spec-matching risk to manage: both artifacts compile the same core module, so identity, signing, and canonical encoding are a single piece of code rather than a specification two separate implementations must independently satisfy.

**What this costs, stated plainly:** Go's WASM output carries a meaningful size baseline — the Go runtime and garbage collector add several megabytes even for a small app, affecting initial page load time, particularly on slow connections. **TinyGo** can reduce this substantially but is not adopted by default here, because its standard-library and cgo-free package coverage needs to be verified against this project's specific dependencies (`pion/webrtc`, `go-app`, the self-owned IndexedDB wrapper's use of `syscall/js`) before being relied on — a Phase 1 validation item (§13), not an assumption.

---

## 4. Core Library Choices (all open source)

### Shared core (both targets)

| Concern | Package | Why |
|---|---|---|
| Identity / signing | `crypto/ed25519` (stdlib) | Keypair = identity, sign every event; identical behavior on both targets since it's the same code |
| Key derivation from passphrase | `golang.org/x/crypto/argon2` | Encrypt private key at rest with a user passphrase |
| Content hashing | [`lukechampine/blake3`](https://github.com/lukechampine/blake3) | Event IDs and media content addressing (§7.2) |
| Serialization | [`fxamacker/cbor`](https://github.com/fxamacker/cbor) | Canonical, deterministic CBOR encoding for signed events (§7.2) |
| Browser-facing WebRTC | [`pion/webrtc`](https://github.com/pion/webrtc) | Same import and API on both targets; a full implementation natively, a thin `syscall/js` wrapper over the browser's own WebRTC stack when built with `GOOS=js GOARCH=wasm` |

### Browser-only (WASM)

| Concern | Package | Why |
|---|---|---|
| UI framework | [`go-app`](https://github.com/maxence-charriere/go-app) | Declarative, component-based Go framework built specifically for compiling to WASM; PWA support, no HTML/JS authored by hand |
| Local persistence | Self-owned `syscall/js` wrapper around IndexedDB (no third-party dependency) | The two candidate community libraries (`hack-pad/go-indexeddb`, and `paralin/go-indexeddb`, which explicitly defers to hack-pad's as "the recommended one") are both stale — hack-pad's last release was February 2023 with no activity since. For something backup-critical (§8), depending on either is worse than owning roughly 200–300 lines of narrowly-scoped binding code (open, get, put, delete, iterate-by-prefix — a plain KV usage, not the full IndexedDB spec surface), matching the same low-dependency philosophy already applied to backup file download/upload. See below for a real hazard this wrapper must handle explicitly. |
| File download/upload for backup | `syscall/js` (stdlib), via the browser's `Blob`/`File`/anchor-download APIs | Native browser download/upload |

**A real hazard this wrapper must handle, independent of who owns the code:** IndexedDB transactions auto-close if left inactive, and Go's WASM runtime can unwind the call stack back to the browser's event loop when a goroutine yields (e.g. blocking on a channel mid-transaction) — the same underlying interaction between IndexedDB's transaction lifetime and Go's WASM scheduler that `paralin/go-indexeddb`'s own documentation describes as a frequent, reproducible failure mode ("transaction is not active"), not an edge case. This isn't a defect specific to either abandoned library; it's a property of combining IndexedDB with Go's WASM goroutine model, and the self-owned wrapper needs to be structured around it deliberately — keeping each transaction's operations synchronous and non-yielding for its duration, or explicitly restarting a transaction if it lapses — rather than assumed away by not depending on a third-party library. Worth validating directly in Phase 1 (§13).

### Native-only (`driftnode`)

| Concern | Package | Why |
|---|---|---|
| NAT traversal / native-to-native transport | [`tailcat`](https://tailscale.com/tailcat) (`github.com/tailscale/tailcat`) | Direct library import — WireGuard encryption, `magicsock` hole-punching, DERP relay fallback (§5.1) |
| DHT / zen discovery | [`go-libp2p`](https://github.com/libp2p/go-libp2p) `kad-dht` | The original, most mature `libp2p` implementation; used purely for discovery, independent of the transport that ultimately carries data |
| Embedded storage | [`bbolt`](https://github.com/etcd-io/bbolt) | Ordered embedded KV store for `driftnode`'s local state |
| CLI framework | [`cobra`](https://github.com/spf13/cobra) | Subcommands, flags, shell completions |
| TUI framework | [`bubbletea`](https://github.com/charmbracelet/bubbletea) + [`lipgloss`](https://github.com/charmbracelet/lipgloss) | Modern, actively maintained Go TUI/styling toolchain (§12.3) |
| Structured logging | `log/slog` (stdlib) | No external dependency needed for structured logs |
| Config/data dir resolution | [`adrg/xdg`](https://github.com/adrg/xdg) | XDG-compliant paths for config, keys, storage, control socket |
| Local control-socket IPC | `net.Listen("unix", ...)` (stdlib) | Daemon exposes a control API; CLI/TUI are thin clients over it |

### WebRTC signaling relay (standalone companion process)

| Concern | Component | Why |
|---|---|---|
| Minimal signaling relay | A small custom Go service (stateless WebSocket relay for SDP offers/answers and ICE candidates) | Now that both the browser and native builds share `pion/webrtc`, a same-language signaling relay is trivial to own outright — a small, self-contained piece of code, still never touching post content, still optional and independently deployable (§5, §6) — rather than taking on an external dependency for a job this simple |

Content-addressed media is chunked and streamed as just another message type over whichever connection is already established for event-log sync (§7.3) — a Tailcat tunnel between native zens, or a WebRTC data channel for browsers — rather than a separate, dedicated blob-transfer library.

Total dependency surface is small and auditable by design; the shared core table above is, by construction, identical code on both targets.

---

## 5. Native Zen Node

Browsers can dial out but cannot accept inbound connections. That constraint is deliberate for casual users — zero attack surface, no port-forwarding, nothing listening on their machine — but it means some jobs require a zen that *can* accept inbound connections. That role is filled by a single artifact, `driftnode`, not a deployment tier: it is the same binary and the same behavior whether it runs on a VPS, a Raspberry Pi, or a laptop.

**These additional jobs specifically require a *reachable* zen:**

| Job | Why it needs a reachable zen |
|---|---|
| Bootstrap/rendezvous entry | A stable, dialable address for new nodes to find first |
| Relaying or mirroring content for other people | Serving someone else's content while they're offline requires strangers to be able to dial in |
| Full Kademlia DHT participation (server mode) | DHT routing needs long-lived, addressable nodes; a non-reachable node still participates as a DHT client |
| WebRTC signaling | Handshake relay for browser↔browser discovery beyond direct invite (§4) |
| Blob hosting for others | Serving media to zens who were offline when it was originally posted, over the same connection mechanism used for event-log sync (§4) |

Reachability for these jobs is close to automatic by default (§5.1) — most operators never need to configure anything for it.

**Implementation notes:**
- **Binary:** `driftnode` is a Go binary built from the shared core module plus native-only packages (§3, §4).
- **Who runs it:** anyone, on any machine capable of running a Go binary — an individual for their own always-on presence, a volunteer who additionally takes on the public seed/bootstrap role (§5.2), or the project's own genesis seed set (§6.2). No special privilege attaches to running one; it holds no data its operator didn't already choose to publish.
- **Resource footprint target:** comfortable on the cheapest available VPS tier (~512MB RAM, single core); the same target applies on a Raspberry Pi — an ARM build is a `GOARCH=arm64 go build` target, not a separate code path. §7.4 defines the storage retention policy that keeps this target true over time, not just at launch.
- **Trust legibility:** the UI distinguishes zens by connection quality, not by hardware class — a zen reached via a direct hole-punched tunnel is shown as such; one reached only via DERP relay is shown as relayed. This is what the TUI's zens panel (§12.3) reflects.
- **The WebRTC signaling relay (§4) is a separate, optional companion process** — run alongside `driftnode` only when its operator chooses to offer browser-discovery signaling. `bootstrap.yaml` lists these under `seed_relays`, distinct from `seed_zens` (§6.1), because a node can run one without the other.
- **Distribution:** prebuilt binaries (GitHub Releases, including ARM targets) and a Dockerfile — running a node is a single downloaded binary or a one-line `docker run`.

### 5.1 NAT Traversal via Tailcat

Native-to-native connection establishment uses [Tailcat](https://tailscale.com/tailcat), Tailscale's open-source, account-free reuse of its data plane: WireGuard encryption, `magicsock` NAT hole-punching, and DERP relay as a fallback path when hole-punching fails. Because `driftnode` is Go, this is a direct library import — no subprocess, no FFI boundary.

**What this replaces, precisely:** the *point-to-point connection establishment and NAT traversal* between two native zens who already know each other's identity. `go-libp2p`'s `kad-dht` remains the discovery mechanism (finding *which* zen to dial); Tailcat only replaces the *how* of dialing once a target is known. This distinction matters because Tailcat's own model — one listener, one connector, an out-of-band token — is single-connection, netcat-shaped, not a discovery or multi-zen mesh protocol.

**How it fits the design:**
- Each `driftnode` runs a Tailcat listener, producing a token (`tc1q...`) that encodes a WireGuard public key and a DERP region — a *routing descriptor*, not an identity. It is published as the zen's reachability record (§7.5) in place of a `libp2p` multiaddr.
- To sync with a known zen, `driftnode` resolves their current token via the DHT and connects to it directly through the Tailcat library. The resulting encrypted duplex stream carries the event-log sync protocol described in §7.3 — Tailcat is a byte-stream transport underneath that protocol, not a replacement for it.
- **Reachability is close to universal by default and requires no volunteer configuration**, because DERP relay-of-last-resort is provided by the project's own maintainer-run relays (below), not something each operator needs to stand up.
- Two keypairs exist per identity, serving different purposes: the Ed25519 keypair (§7) signs application events and *is* the identity; a separate WireGuard (X25519) keypair, generated and held by Tailcat, authenticates the transport tunnel. This mirrors how Signal-style protocols separate an identity key from a session/transport key, and keeps the transport swappable without touching identity.

**DERP relays are maintainer-run by default, not a per-volunteer responsibility.** DERP's own design intent is a small number of well-placed regional relays, not one per node — true of Tailscale's own production deployment. DERP's server code is open source and self-hostable (the `derper` binary, widely used in the Headscale ecosystem) and lightweight enough that a handful of maintainer-operated instances (real-world reports: roughly 100MB RAM for around 50 concurrent zens on a single relay) comfortably serve the network's relay-of-last-resort needs. These are published as `derp_relays` in `bootstrap.yaml` (§6.1), the same pattern as `seed_relays` and `seed_zens` — swappable, forkable, and never touching plaintext (a DERP relay only ever sees WireGuard-encrypted bytes, the same trust category as the WebRTC signaling relay). This is what makes volunteering effortless: an operator runs `driftnode`, and NAT traversal works immediately against the project's own relay fleet — no self-hosting, no port forwarding, no configuration.

**One implementation detail flagged, not yet settled:** `go-libp2p`'s Kademlia DHT still needs a registered transport of its own to carry DHT control traffic (zen lookups, provider record publishing) — it does not run as pure, transport-independent logic. Two options: run a lightweight `go-libp2p` QUIC transport solely for DHT traffic alongside Tailcat for the actual event-log sync data, or wrap a Tailcat-established tunnel as a custom `go-libp2p` transport, unifying both under one mechanism. The latter is architecturally cleaner but is real implementation work to validate in Phase 0, not a given.

**Not yet validated, flagged for Phase 0 (§13):** whether a single Tailcat listener cleanly handles many concurrent inbound connections from different zens, or whether `driftnode` needs to run multiple listeners or add its own connection multiplexing on top. Tailcat's documentation describes a single listener/single connector shape; the gossip network's actual concurrency needs should be tested directly rather than assumed.

### 5.2 Becoming a Public Seed or Bootstrap Entry

Tailcat and maintainer-run DERP make basic reachability close to automatic, but there is still a genuinely opt-in role left: being *listed* as a stable, public entry point in `bootstrap.yaml` or the DHT's bootstrap set (§6). That is a decision about identity persistence and public visibility, not a NAT problem:

1. A bootstrap/seed entry needs a **long-lived Tailcat keypair** rather than the ephemeral one generated by default on each run, so its published token stays valid across restarts.
2. Listing is an explicit action — `driftnode relay enable` publishes the node's token into the DHT as a bootstrap/relay candidate and keeps it stable across restarts. Nothing does this automatically.
3. `driftnode relay status` reports current state: ephemeral (default), or enabled-with-stable-identity.
4. None of this affects a non-volunteering node's own experience. Dial-out sync, local crawling, and posting behave identically whether or not `relay enable` was ever run — this only changes what a node does *for other people*.

---

## 6. Rendezvous & Bootstrap

Two zens with no shared history cannot open a direct connection without some way to exchange addresses. Three mechanisms, in order of how much they depend on anyone but the two zens involved:

1. **Direct invite** — a link or QR code carrying a WebRTC offer and pubkey (comparable to Signal's safety-number exchange or Scuttlebutt's pub invite). The recipient's client generates an answer and the connection completes with zero infrastructure. Manual, but sufficient for "add a friend."
2. **Community signaling relays** — for discovering people not already known, the WebRTC signaling relay (§4) holds only ephemeral connection metadata, never post content or a social graph, and is trivially self-hostable. A relay is a transient routing helper only — never part of anyone's identity or address (§7.5).
3. **DHT bootstrap** — native zens use `go-libp2p`'s `kad-dht`, seeded from a small published list of bootstrap addresses (the same pattern as BitTorrent or IPFS). This is also where a browser client gets its first list of native zens to try.

None of these mechanisms store user data; they are closer to DNS root servers or STUN servers than to a platform.

### 6.1 The Signed `bootstrap.yaml`

A static, signed file is how a fresh install — browser or native — finds its first zens, without any lookup service being run on its behalf. The zen and relay entries are routing hints only, for a small set of seed nodes; they are never a lookup mechanism for arbitrary user identities, which is resolved dynamically via the DHT instead (§7.5):

```yaml
version: 1
signature: "ed25519:..."      # signed with the project's well-known pubkey
seed_relays:
  - url: "wss://relay1.example.org"
    kind: webrtc_signaling
derp_relays:
  - region: "project-eu"
    url: "https://derp1.example.org"   # maintainer-run, the default relay-of-last-resort — §5.1
seed_zens:
  - token: "tc1qabc...xyz"   # long-lived Tailcat token for a seed native zen, §5.1/§5.2
    kind: native_zen
crawl_seeds:
  - "driftnode:9f2a…c001"   # high-in-degree accounts to start the built-in crawl from, §9.3
```

- **Verified content, not trusted location:** the client checks the Ed25519 signature against a pubkey compiled into the binary. Because the content is verified, it doesn't matter which mirror served it — GitHub raw, an IPFS gateway, a DNS TXT record — tampering by any single host is detectable and rejected.
- **Degrades gracefully:** fetch order is (1) a cached copy from the last successful run, (2) a network fetch from any configured mirror, (3) a small hardcoded fallback list compiled into the binary. Once connected, zens exchange their own routing tables via Kademlia, so `bootstrap.yaml` is a cold-start convenience, not an ongoing dependency.
- **Publication:** the project ships an initial signed file pointing at its own seed zens and relays. Anyone can fork it, sign their own copy with their own key, and point a build at a different file — the same trust model as a Linux distribution's package-signing key.

### 6.2 Network Genesis

At the network's genesis there is no follow-graph to crawl. The maintainers' own accounts are the entire seed dataset — temporary and self-correcting, the same way every social network, centralized or not, starts.

**Before any external user exists:**
1. Maintainers stand up the initial seed native zens and DERP relays (§13, several regions).
2. Each maintainer creates a normal `driftnode` identity on those nodes — there is no special "admin account" type; a maintainer's pubkey is structurally identical to anyone else's.
3. They follow each other and post, so the seed graph contains actual content, not just empty profiles.
4. They sign `bootstrap.yaml` v1 with `seed_zens`, `derp_relays` pointing at their own infrastructure, and `crawl_seeds` set to their own pubkeys — the entire genesis dataset.

**As external users join:**
5. New clients fetch and verify `bootstrap.yaml`, dial `seed_zens`, and run the built-in crawl (§9.3) starting from `crawl_seeds` — at this point that crawl can only discover the maintainers and whoever has followed them so far. Search results and any "suggested accounts" surface are transparently just the maintainers at this stage; the UI states this rather than obscuring it.
6. As these users follow each other, not only the maintainers, the follow-graph gains structure independent of the seed accounts for the first time.

**Steady state — `crawl_seeds` evolves rather than stays pinned:**
7. Maintainers periodically re-run their own daemon's crawl (the same mechanism every node has, §9.3) against the live network and select a new `crawl_seeds` list algorithmically — for example, top-N by in-degree among accounts with some minimum age and activity, filtered for obvious spam. No new machinery is built for this.
8. They sign and publish `bootstrap.yaml` v2, v3, and so on. Each version's `crawl_seeds` should look progressively less maintainer-centric as the organic graph grows — that drift is the actual signal that decentralization is taking hold.
9. Once a node has crawled once, `crawl_seeds` stops mattering to it — the same cold-start-only property as the zen bootstrap list (§6.1). It only ever affects a brand-new user's first session.

**Governance, named plainly:** the `bootstrap.yaml` signing key is real, narrow influence — whoever holds it chooses what a brand-new user sees first, and which DERP relays a fresh install trusts by default. It cannot touch anyone's data, posts, or follow-graph after that first crawl. Mitigations: an operator can override `crawl_seeds` or `derp_relays` locally, or import a friend's exported crawl cache instead of the default; onboarding can offer a choice of independently-signed starter lists (maintainer picks, a community-curated list, or an empty start using direct-invite only, §9.4); anyone can fork the bootstrap file with their own signing key and run their own DERP fleet.

In-degree-based seed selection is Sybil-gameable — anyone can generate unlimited pubkeys and follow each other. Weighting search results by "followed by people you already follow" (§9.4) is a partial local mitigation. Full Sybil resistance is a harder problem shared by every P2P social graph and is out of scope for v1.

---

## 7. Data Model & Identity

- **Identity string:** `driftnode:<base32-pubkey>` — the entire address. No hostname, no relay, no routing detail is ever encoded in it, because routing information changes (an operator switches native zen, a relay goes down, a device moves networks) while identity must not.
- **Signing:** every event is signed with its author's private key before being added to their log, so zens can verify authenticity independent of how they received it — browser, relay, or otherwise. Only events signed by an identity's own key are ever accepted into that identity's log, which is what prevents anyone else from forging posts, follows, or deletes regardless of which zen relayed them.
- **No CRDT engine, by design.** Each identity's log has exactly one legitimate writer — its own signing key (nobody else can produce a validly-signed event for it) — so there is never a concurrent-write conflict between two authors to reconcile. "Merging" two zens' views of a log is set union over immutable, individually-signed events, not CRDT-style conflict resolution. This is the same model Nostr and Secure Scuttlebutt use.
- **One implementation, not two.** Because the shared core module (§3) is compiled unmodified into both the browser and native builds, the event model, canonical encoding, and signing/verification logic below are a single piece of code, not a specification two separate implementations must independently satisfy.
- **Media:** attachments are content-addressed (BLAKE3 hash) and fetched lazily as chunked transfers over whichever connection is already established for event-log sync — a Tailcat tunnel between native zens, or a WebRTC data channel between browsers (§4) — never inlined into an event, keeping logs light regardless of media volume.

### 7.1 Two Logs Per Identity

Each identity maintains two independent event logs, so that discovering someone never requires downloading their content history:

| Log | Contents | Synced by |
|---|---|---|
| **Profile log** | `Profile` (zen name, avatar hash — last-write-wins by timestamp), `Follow`/`Unfollow` events — small, low-churn | Anyone; this is what the built-in crawler (§9.3) fetches and caches durably |
| **Detail log** | `Detail` (bio, first name, last name, location — last-write-wins by timestamp) | Anyone who asks, but **never crawled and never cached durably**: a peer fetches it on demand to display someone's full profile and discards the events afterwards. The owner's own Detail log is backup-critical like the other own logs. |
| **PostLog** | `Post`, `Reply`, `Like`, `Delete` events — the content stream, potentially large and long-lived | Only zens who follow this identity (§9.1) |

Because each log is just a named, independently-requestable stream of signed events, a zen can ask for "give me `driftnode:<pubkey>`'s Profile log" without ever touching their PostLog or Detail log. This is what makes the crawler's low cost an architectural property rather than a policy: a crawl of thousands of accounts never requests, and therefore never receives, anyone's post history or personal details.

**Current state is a deterministic projection over the log**, computed independently by every zen from whatever subset of events it has: the current profile is the latest `Profile` event by timestamp; the current details are the latest `Detail` event by timestamp; the current follow set is the result of replaying `Follow`/`Unfollow` events in order; the current post list is every non-tombstoned `Post`/`Reply` event. Because projection is a pure function of the event set, two zens with different partial views only ever differ in *completeness*, never in *disagreement* about what a given event means.

### 7.2 Event Types & Post Addressing

| Event | Lives in | Fields (indicative) |
|---|---|---|
| `Profile` (upsert) | Profile log | `display_name`, `avatar_hash`, `timestamp` |
| `Detail` (upsert) | Detail log | `bio`, `first_name`, `last_name`, `location`, `timestamp` |
| `Follow` / `Unfollow` | Profile log | `target_pubkey`, `timestamp` |
| `Post` | PostLog | `text`, `media_hashes[]`, `timestamp` |
| `Reply` | PostLog | same as `Post`, plus `parent_id` |
| `Like` | PostLog of the **liker**, not the target | `target_id` |
| `Delete` | Any log | `target_id` (tombstone) |

**Every event has a canonical serialized form** — a fixed field order, deterministic CBOR encoding — produced by the shared `fxamacker/cbor`-based encoding code in the core module (§3). Because both targets run this same code, there is no cross-implementation encoding drift to guard against; the canonical form is simply whatever the shared function produces. It is still worth a short written spec plus test vectors as living documentation and a regression guard, but not as a mechanism to keep two implementations in agreement.

**Post/event IDs** are the BLAKE3 hash of an event's canonical signed, serialized bytes. `Reply.parent_id` and `Like.target_id` both point to this hash; a reference is only resolvable once the referenced author's PostLog has actually been synced.

**Likes and counts are observational, not authoritative.** Because a `Like` lives in the liker's own log — nobody can write into someone else's log without a valid signature from that identity's own key — a "like count" on a post is always "the number of `Like` events this particular zen has observed referencing this ID," never a true global count. The same caveat applies to follow-counts surfaced by the crawler (§9.3).

**`Delete` is a tombstone, not erasure.** It is itself a signed event referencing the target's ID. Zens who already replicated the original content before a delete propagated may still hold a copy — a structural property of gossip replication.

### 7.3 Wire Protocol

Synchronizing a log between two zens is a set-reconciliation problem, not a CRDT merge: each side needs to learn which signed events the other has that it doesn't, and transfer only those. The baseline mechanism — sufficient for Phase 0 (§13) and revisited only if it proves too coarse in practice — is a timestamp cursor: "send me every event in log X after time T," with the requester's last-synced cursor persisted locally per zen per log. A more precise set-reconciliation scheme (e.g. exchanging sorted event-ID ranges or a compact set sketch) is worth evaluating once real usage patterns exist, but is not a blocker for an initial correct implementation. Messages are framed as length-prefixed CBOR and sent over whichever byte stream the connection-establishment mechanism provides — a Tailcat tunnel between native zens (§5.1), or a `pion/webrtc` data channel for browser zens (§4). Because the sync protocol only depends on a byte-stream abstraction and both ends run the same shared implementation, bridging these different mechanisms on `driftnode` (§2, §5) requires no protocol translation.

### 7.4 Storage Growth & Retention

Given the ~512MB RAM / cheap-VPS target (§5), storage growth is bounded explicitly:

| Data class | Retention policy |
|---|---|
| PostLogs of followed accounts | Retained fully by default. Optional `postlog_retention_days` config caps local history for operators who want it — pruned locally only, never removed network-wide, since zens who still hold it can still serve it |
| Profile logs from crawling (not followed) | Bounded LRU cache, capped by `crawl_cache_max_entries`, evicted oldest-touched-first |
| Detail logs of others | Never stored durably. Fetched on demand for display and discarded immediately, so a seed following thousands of zens never accumulates their personal details on disk. |
| Media blobs | Reference-counted against retained PostLogs; garbage-collected once unreferenced; hard cap `media_cache_max_bytes` as a backstop |
| DHT routing table | Bounded by `go-libp2p`'s `kad-dht`'s own k-bucket limits; no additional policy needed |

These are `driftnode config` knobs (§12.2) rather than architectural exceptions — a personal node and a heavily-connected public relay set them differently, using the same code path.

### 7.5 Resolving Identity to Routing

"Where do I currently reach `driftnode:a3f9…21c4`" is answered dynamically, never baked into the identity string:

- The DHT (`go-libp2p`'s `kad-dht`) stores a **signed provider record** per pubkey: the identity's owner, or whichever native zen is currently relaying for them, publishes their current routing descriptor — a Tailcat token for a native zen, or a native zen's token for a relayed browser user — signed by the pubkey's own key so a relay cannot spoof the record.
- Native zens publish their own current Tailcat token (§5.1).
- Browser-only users have their currently-connected native zen publish a record on their behalf — still signed by the user's own key, with the native zen only relaying the publish operation — pointing at that native zen's Tailcat token as the current mirror path.
- Records carry a short TTL and are refreshed periodically — Tailcat tokens for ephemeral (non-seed) zens can legitimately change on every restart — so switching relays, restarting, or changing networks produces a new record while the identity string itself never changes.
- This is the same primitive IPFS calls a provider record and `go-libp2p` calls a zen record, applied to user identities instead of content hashes; only the payload (a Tailcat token instead of a multiaddr) differs from the general pattern.

---

## 8. Local Storage & Backup

- **Live storage:** an identity's Profile log, Detail log, own PostLog, and the media cache live in IndexedDB (browser, via the self-owned wrapper, §4) or `bbolt` (native, `driftnode`), persisting across restarts with no configuration. Followed accounts' synced PostLogs and any crawled Profile logs are re-fetchable cache, not backup-critical, and are excluded from the export below to keep it small. Others' Detail logs are never stored at all (§7.4). Both storage backends serialize events using the same shared canonical-encoding code (§7.2), so a backup produced on one platform is trivially readable by the other.
- **Export:** serializes the account-critical state — the keypair (encrypted with the user's passphrase), the user's own Profile log, Detail log, own PostLog, and their local petname map (§9.4) — into a single CBOR file. In the browser this triggers a native download via the `Blob`/anchor-download APIs (`syscall/js`); on native, `driftnode backup export` writes it directly. Only the keypair is encrypted; the logs are already public, signed content, so re-encrypting them adds no protection while making the file harder to inspect for debugging.
- **Import:** reads the CBOR file, decrypts the keypair with the user's passphrase, and merges all logs into local storage as a set union of signed events (§7). Because events are immutable and individually verifiable, importing an older backup and continuing to sync with zens holding newer state merges cleanly rather than overwriting or conflicting.
- **This backup file is the account.** There is no password reset and no account recovery outside it — the same tradeoff as any self-sovereign identity system (SSH keys, cryptocurrency wallets). The UI states this loudly and repeatedly, not just once during onboarding.

---

## 9. Discovery, Search & Feed Construction

### 9.1 Feed

Entirely client-side, with no ranking server:
1. The client holds the PostLogs (§7.1) of everyone the user follows, synced via gossip (§7.3).
2. The timeline is a merge-sort by timestamp across those logs, resolving `Reply.parent_id` references where the parent event has also been synced (surfacing "reply to a post you haven't synced" otherwise). Reverse-chronological by default, with a pluggable local scoring function available on top.

### 9.2 Why Search Cannot Be Global

There is no central index, so there is no query that returns *the* authoritative "alice" — display names are not unique and cannot be made unique without reintroducing a central naming authority (Zooko's triangle: human-readable, globally unique, decentralized — pick two). Search is always best-effort, sourced from whatever a given zen has crawled, and the UI communicates that rather than presenting results as authoritative.

### 9.3 Built-In Crawling

Rather than an opt-in class of index-zen nodes, every native daemon crawls the public follow-graph as ordinary background behavior — an emergent property of participating, not an assigned role (§6.2 covers how this bootstraps from nothing at genesis):

- `bootstrap.yaml`'s `crawl_seeds` (§6.1) list a handful of high-in-degree accounts to start from.
- The daemon performs a breadth-first walk of the follow-graph, syncing only Profile logs (§7.1) — never PostLogs — for each seed account, then their followers and follows outward, up to a configurable depth and the `crawl_cache_max_entries` cap (§7.4).
- This is cheap by construction, not by policy: Profile logs contain no post history, so crawling thousands of accounts never touches anyone's actual content stream.
- A node with more uptime and storage accumulates a bigger, more useful crawl cache purely as a side effect of participating longer — the same way a well-seeded BitTorrent zen ends up caching more of the swarm. No node is assigned the job of indexing.
- Browser clients do not crawl themselves — no background compute or storage budget for it — and instead query whichever native zen(s) they are connected to, receiving answers from that zen's local crawl cache.
- "Follow-count" surfaced in search results (§9.4) is a given zen's *observed* count from its own crawl, not a global truth — the same caveat as the Like-count observation in §7.2.

### 9.4 Search & Follow

**Tier 1 — the identity is already known:** shared directly as `driftnode:<pubkey>` via link or QR code. The client verifies the signature matches, resolves current routing via the DHT (§7.5), dials, and follows. This is the only tier with cryptographic certainty.

**Tier 2 — searching by name:**
1. The user types a query, e.g. "alice."
2. The client asks its connected native zen(s) to search their local crawl cache of Profile logs — no special server, only whichever zens are already connected.
3. Results show display name, pubkey, an identicon, and light social proof ("followed by 2 people you follow"), explicitly labeled by which zen's crawl cache produced them.
4. Selecting a result resolves that exact pubkey's current address via DHT lookup (§7.5), dials directly, and syncs its Profile log (and PostLog, if the user chooses to follow). The zen that helped find the result drops out of the trust path immediately — it was used only for discovery, never for verification.
5. The UI foregrounds the pubkey/identicon rather than only the display name (the same principle as Signal's safety numbers), so visually similar names backed by different keys remain easy to distinguish.

**Tier 3 — cold start, no follow-graph yet:** `bootstrap.yaml`'s `crawl_seeds` double as an explicitly editorial, opt-in "suggested accounts" list, the pattern early Mastodon instances used — never silently auto-followed.

**After following — local petnames:** a followed identity can be given a local nickname, stored only in the user's own Profile log (Scuttlebutt's "petname" model — this is why petnames live in the Profile log rather than the PostLog: they are metadata about the user's own graph, not content). This carries the day-to-day readability burden; global search remains fuzzy and best-effort by design, while a user's own follow list stays exact.

---

## 10. Moderation & Spam

- Mute/block lists are client-side and themselves shareable as signed event logs — subscribing to a friend's or a community's blocklist, the same pattern Bluesky uses for labelers, but zen-published rather than server-published.
- Proof-of-work or rate-limited signing acts as a cheap, local speed bump against spam on the gossip layer. No server-side enforcement is possible or intended.

---

## 11. Tradeoffs

| Tradeoff | Why it is accepted |
|---|---|
| No real-time guaranteed delivery | Inherent to a gossip, offline-first design |
| Search is always best-effort and zen-sourced, never authoritative | Zooko's triangle rules out a central index without reintroducing a central authority; built-in crawling (§9.3) at least makes this cheap and role-free |
| Like counts, follow counts, and similar aggregates are observational, not global truths | Each zen only knows what it has synced or crawled (§7.2, §9.3); no component is positioned to compute a true global count without becoming a central index |
| A lost backup file is a lost account | Same as any self-custodied key; mitigated by encouraging multi-device sync while online |
| Native zen nodes are a required part of the network, not optional polish | Browsers structurally cannot accept inbound connections — accepted as the cost of casual users having zero attack surface (§5) |
| Go's WASM output carries a meaningful size baseline | The Go runtime and GC add several megabytes even for a small app; accepted in exchange for one shared implementation instead of two that must stay in sync (§3). TinyGo is a possible future mitigation, not yet validated against this project's specific dependencies |
| No actively maintained third-party Go/WASM IndexedDB library exists | Rather than depend on one of the two stale candidates found, the browser storage layer is a small, self-owned `syscall/js` wrapper (§4) — more upfront work, but no external maintenance risk for something backup-critical, and full control over handling the transaction-lifetime hazard described in §4 |
| Maintainers must operate a small DERP relay fleet as ongoing infrastructure | The alternative — depending on Tailscale's own DERP fleet by default, or requiring every volunteer to self-host one — is worse on the "no company-owned backend" goal (§1) or on volunteer friction, respectively; a handful of maintainer-run relays is the least-bad option and matches DERP's own intended deployment shape (§5.1) |
| Tailcat's connection model is single listener/single connector, not natively multi-zen | Needs direct field validation (§13) of whether one listener handles many concurrent zens or whether `driftnode` needs its own multiplexing on top |
| Native zens behind consumer NAT still occasionally can't be reached even via DERP | Rare in practice given relay-of-last-resort, but not eliminated; the node still works fully for its own operator regardless (§5.1) |
| No private messaging in v1 | The public-log data model (§7) is not a variant of DM infrastructure; that would need a separate key-exchange system, out of scope for now (§1) |
| Some rendezvous mechanism is unavoidable | Minimized to a signed static file plus swappable, non-data-holding relays (§6), never encoded into identity itself (§7) |

---

## 12. `driftnode`: CLI & TUI

### 12.1 Process Shape

A single long-running daemon holds the network state — active Tailcat tunnels to native zens (§5.1), `go-libp2p`'s `kad-dht` routing table, gossip subscriptions. The CLI and TUI are both thin clients of that daemon over a local control socket, rather than each invocation opening its own connections from cold.

```
driftnode daemon              # long-running: tailcat, kad-dht, gossip, control socket
driftnode <command> [args]    # thin client → control socket, falls back to direct
                                   #   bbolt access for offline/scriptable commands
driftnode tui                 # bubbletea client → same control socket, live updates
```

- **Control socket:** a Unix domain socket (named pipe on Windows) at `$XDG_RUNTIME_DIR/driftnode/control.sock`, carrying a JSON-lines protocol distinct from the CBOR wire format used peer-to-peer. Local-machine only, never exposed to the network.
- **Offline fallback:** commands that don't require live network state (`post`, `whoami`, `key export`, reading a cached `feed`) operate directly on the local `bbolt` store if no daemon is running, queuing anything that needs to propagate for the next sync. Commands that inherently need the network (`zens`, `sync --now`, `bootstrap fetch`) require the daemon and say so clearly if it isn't running.

### 12.2 CLI Command Spec

| Command | Purpose |
|---|---|
| `driftnode init` | Generate an Ed25519 keypair, passphrase-encrypt it at rest, create the local `bbolt` store |
| `driftnode whoami` | Print the identity and its current profile and detail |
| `driftnode daemon [--foreground]` | Start the long-running daemon and control socket (default detached; `--foreground` for field testing with logs on stdout) |
| `driftnode post "<text>" [--media <path>]` | Append a signed `Post` event to the local PostLog |
| `driftnode feed [--limit N] [--follow-only]` | Print the merged timeline from local state, no network call |
| `driftnode follow <pubkey>` / `unfollow <pubkey>` | Add or remove a follow edge, a signed Profile-log event |
| `driftnode profile --name <zen-name>` | Set the public profile (zen name) shown in the crawl cache (§7.1) |
| `driftnode detail [--bio <text>] [--first-name <name>] [--last-name <name>] [--location <place>]` | Set personal metadata (Detail log, fetched on demand and not cached durably by peers, §7.1) |
| `driftnode zens list` | Show currently connected zens, their kind (native/relay/browser), and latency |
| `driftnode follow <token-or-pubkey>` | Follow an identity: dial and follow if given a token, or follow by pubkey offline |
| `driftnode relay status` | Show the reachability check result and whether the relay role is enabled (§5.1, §5.2) |
| `driftnode relay enable` / `relay disable` | Explicitly opt in or out of advertising this node as a relay/bootstrap candidate |
| `driftnode sync [--now]` | Trigger an immediate sync round instead of waiting for the daemon's interval |
| `driftnode bootstrap fetch [--url <mirror>]` | Fetch and verify `bootstrap.yaml`, merging it into the known-zens cache |
| `driftnode bootstrap verify <file>` | Verify a local copy's signature without applying it |
| `driftnode backup export <path>` | Write the single-file CBOR backup (§8) |
| `driftnode backup import <path>` | Restore or merge from a backup file |
| `driftnode key export` / `key import` | Raw keypair export/import, separate from a full backup |
| `driftnode config show` / `config set <k> <v>` | View or edit local configuration |
| `driftnode log [--follow]` | Tail the daemon's structured log output |
| `driftnode tui` | Launch the terminal UI |

Built with `cobra`, so `--help` output and shell completions come for free.

### 12.3 TUI Layout

A `bubbletea` app (styled with `lipgloss`) with four panels, all fed by the same control-socket subscription — the daemon pushes sync and zen-connection events, and the TUI does not poll:

```
┌─ driftnode · you: a3f9…21c4 ─────────────────────────────┬─ Zens (3) ──────────┐
│                                                          │ ● seed-eu (native)  │
│  [feed — reverse chron, merged across follows]           │ ● seed-us (native)  │
│  alice> gm from the field test                    2m     │ ○ bob (browser, wrtc)│
│  bob> anyone else seeing sync lag on the eu relay? 5m    │                     │
│  you>  posted: "testing native-first bootstrap"    9m    ├─ Status ────────────┤
│  ...                                                     │ sync: ok, 4s ago    │
│                                                          │ sync round: #218    │
│                                                          │ bootstrap: verified │
├─ compose ────────────────────────────────────────────────┴──────────────────┤
│ > _                                                                          │
├─ log (tail -f daemon) ────────────────────────────────────────────────────────┤
│ 12:04:01 INFO  sync: merged 3 events from seed-eu                            │
│ 12:04:05 INFO  kad-dht: routing table now has 7 zens                        │
└───────────────────────────────────────────────────────────────────────────────┘
```

- **Feed panel:** live-updating, merge-sorted timeline; scrollable.
- **Zens panel:** connection kind (native, browser-relayed, direct WebRTC), latency, last-sync time — the trust-legibility distinction from §5.
- **Status panel:** at-a-glance daemon health — last successful sync round, bootstrap verification state, DHT routing table size. The single most useful panel during field testing, since it reflects whether the network is working, not just the UI.
- **Compose:** inline post entry; `Enter` signs and appends locally, propagating on the next sync round.
- **Log tail:** embedded structured-log output, so a second terminal isn't needed during field tests.
- **Keybindings:** vim-style — `j`/`k` scroll, `i` to focus compose, `Esc` to unfocus, `q` to quit, `/` to filter the feed by author.

The TUI is purely a presentation layer over the control socket; it adds no logic to the daemon.

---

## 13. Build Order

The native binary and the browser app now share one core module (§3) — identity, event/log model, canonical encoding, signing are written once and compiled into both targets. The native side still goes first, because it carries the hardest-to-debug logic: event-log sync convergence, DHT behavior, and NAT traversal are all fully testable headless, on real machines, over a real network, with a CLI instead of a UI. WASM introduces a second, unrelated set of variables (browser sandboxing, the IndexedDB transaction-lifetime hazard described in §4, `pion/webrtc`'s `js/wasm` maturity for this project's specific usage) that should not be mixed in while the network itself is still being validated.

**Phase 0 — native only, field-testable, no browser involved**
1. Shared core module: identity (`crypto/ed25519`), the Profile/PostLog event-log split, signed event types, canonical CBOR encoding, and content-hash addressing (§7.1–7.2) — unit-testable, no networking yet, no platform-specific code. This module is what Phase 1 later compiles into the browser build unmodified.
2. `driftnode` CLI in offline mode: `init`, `whoami`, `post`, `feed`, operating directly on `bbolt` with no daemon or network — proves the data model in isolation.
3. Daemon process and control socket (§12.1); direct Tailcat integration (§5.1) between two native nodes on separate machines behind real, different NATs — wiring the CLI's `zens`/`sync` commands to it, proving the event-log sync protocol (§7.3) and set-reconciliation correctness under real latency, packet loss, and restarts. This is also where the open question of concurrent-connection handling (§5.1) gets answered rather than assumed.
4. `driftnode tui`: a thin `bubbletea` client over the same control socket, giving a live dashboard for the remainder of field testing instead of raw log reading.
5. Signed `bootstrap.yaml`, `go-libp2p`'s `kad-dht` for discovery, and a maintainer-run DERP relay region (§5.1): 2–3 seed nodes across regions, testing whether strangers' nodes can find and reach each other through it — validating discovery and Tailcat-based NAT traversal across real NATs and ISPs.
6. Built-in crawler (§9.3) against the seed-node graph, validating that Profile-log-only crawling stays cheap in practice before any browser client is involved.

At the end of Phase 0, a working, gossiping, event-log-synced P2P network exists with a CLI and TUI to drive and observe it — the hardest part of "serverless" is validated before any browser work begins.

**Phase 1 — browser client, once Phase 0 is proven**
7. Native `pion/webrtc` listener added to `driftnode`, plus the standalone WebRTC signaling relay (§4) — both required before any browser can connect, since browsers reach the network only through this bridge (§2, §5).
8. Browser build target: the same shared core module from step 1, compiled with `GOOS=js GOARCH=wasm`, plus `go-app` for the UI and the self-owned IndexedDB wrapper (§4) for persistence. Because it's the same core code, this step is a build-target exercise, not a second implementation to validate against the first.
9. The IndexedDB wrapper's handling of the transaction-lifetime hazard (§4) validated directly — writing and reading real event logs under realistic goroutine scheduling, not just a happy-path smoke test — since this is the one place browser-only code carries genuine platform risk.
10. `pion/webrtc`'s `js/wasm` build validated directly against the native listener from step 7, including through the signaling relay and under real NAT conditions — the one genuinely new integration surface this phase introduces, worth its own explicit test pass rather than assuming parity with the native build.
11. Direct-invite sync between two browser tabs — the zero-infrastructure Tier-1 path (§9.4).
12. The browser build pointed at the Phase-0 `bootstrap.yaml` and seed nodes — the first time a browser zen joins the live network.
13. Chunked media transfer over existing connections (§4).
14. Moderation lists and pluggable local feed scoring. Tier-2 search (§9.4) is available once Phase 0's crawler and Phase 1's browser client both exist, since no separate index-zen component needs building.
