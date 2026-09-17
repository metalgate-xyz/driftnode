# driftnode Test Plan

## Containerized end-to-end test

`scripts/test-containers.sh` runs a realistic end-to-end test in isolated
Docker containers (managed by `docker-compose.yml`). It exercises the real
peer-to-peer discovery and sync paths from the design, not a hand-wired
local approximation.

### Prerequisites

- Docker or OrbStack
- AF_ROUTE support in the containers (`--cap-add=NET_ADMIN` is set in the
  compose file)

### Run

```sh
bash scripts/test-containers.sh
```

### What it tests

Five containers, each an isolated node with its own store:

| Node    | Region | Bootstrap      | Role                                    |
|---------|--------|----------------|-----------------------------------------|
| seed-eu | EU     | (none)         | Genesis seed, persistent key            |
| seed-us | US     | (none)         | Genesis seed, persistent key            |
| carol   | EU     | seed-eu only   | Fresh user, discovers seed-us via crawl |
| dave    | US     | seed-us only   | Fresh user, discovers seed-eu via crawl |
| eve     | EU     | seed-eu only   | Fresh user, extra graph structure       |

### Discovery chain

1. Seeds initialize, follow each other, and post. Their tokens are stable
   across restarts (persistent tailcat keys).
2. Fresh nodes load a signed `bootstrap.yaml` listing only their regional
   seed, auto-dial it, and sync (Phase 1).
3. The crawler walks the seed's follow graph, discovering the other seed
   that was NOT in the bootstrap file (Phase 2). Peer exchange relays the
   other seed's token so the fresh node can dial it.
4. Fresh nodes discover each other through peer exchange (the seed relays
   their tokens), follow, and sync posts bidirectionally (Phases 3-4).
5. Eve joins and discovers the network the same way (Phase 5).
6. The seed's tailcat token stays stable across a container restart
   (Phase 6, persistent key).
7. Re-syncing is idempotent (Phase 7).
8. A late post propagates on the next sync round (Phase 8).

### Unit tests

The unit test suite (`go test ./...`) covers the building blocks the
containerized test exercises end-to-end:

- Core: identity, canonical encoding, signing, event/log model
- Store: bbolt CRUD, followed-event dedup by event ID, merged feed
- Sync: wire protocol, bidirectional session, peer exchange, signature
  verification
- Bootstrap: sign, verify, tamper detection
- Crawler: BFS, depth limit, dedup
- Daemon: control-socket RPC, lifecycle, transport wiring
- TUI: model state transitions
