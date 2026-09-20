# driftnode

A serverless, peer-to-peer social network. No company owns your account, your
posts, or your social graph. Your identity is an Ed25519 keypair on your
device. Posts are signed events in an append-only log, replicated peer-to-peer
over encrypted tunnels. There is no central server, no auth service, and no
feed-ranking algorithm.

This repository contains `driftnode`, the native zen binary. It runs on
any machine: a VPS, a laptop, a Raspberry Pi. It holds your identity, your
posts, the posts of people you follow, and a crawl cache of public profile
metadata discovered through the follow graph. It syncs with other nodes over
[Tailcat](https://tailscale.com/tailcat) tunnels (WireGuard-encrypted, with
NAT traversal and DERP relay fallback).

## Status

Phase 0: native-only, field-testable. The full peer-to-peer stack works:
identity, signed event logs, bidirectional sync, zen exchange, follow-graph
crawling, bootstrap auto-dial, and persistent node addresses. A browser
client (compiled from the same Go core to WebAssembly) is planned for
Phase 1.

## Install

### Prebuilt binary

Download from [GitHub Releases](https://github.com/metalgate-xyz/driftnode/releases)
(including ARM builds for Raspberry Pi) and place it on your `PATH`.

### Build from source

```sh
git clone https://github.com/metalgate-xyz/driftnode.git
cd driftnode
go build -o driftnode .
```

### Docker

```sh
docker build -t driftnode .
```

## Quick start

```sh
# Create your identity (Ed25519 keypair, encrypted at rest with your passphrase)
driftnode --db ~/.driftnode/node.db init -p "your-passphrase"
# -> driftnode:9f2a...c001

# Set your public zen name (shown in the crawl cache)
driftnode --db ~/.driftnode/node.db profile -p "your-passphrase" --name "marx"

# Start the daemon (background networking: sync, crawl, zen exchange)
driftnode --db ~/.driftnode/node.db daemon
# In another terminal, unlock the signing key once so the commands below
# don't need a passphrase on every call:
driftnode --db ~/.driftnode/node.db daemon unlock
# -> prompts for your passphrase, keeps the key in memory until lock/stop

# Post something
driftnode --db ~/.driftnode/node.db post "gm, the sun is out"

# Read your local timeline (posts from people you follow, merged by time)
driftnode --db ~/.driftnode/node.db feed

# Follow someone (paste their driftnode: identity string)
driftnode --db ~/.driftnode/node.db follow driftnode:abc123...

# Trigger a sync round and see who you're connected to
driftnode --db ~/.driftnode/node.db sync
driftnode --db ~/.driftnode/node.db zens list
```

When the daemon isn't running, every signing command (`post`, `follow`,
`unfollow`, `profile`, `detail`) falls back to opening the store directly and
needs `-p <passphrase>`.

## How it works

### Zen

A driftnode user is a *zen*: a citizen without a city. The city is the
central server, and driftnode has none, so nothing holds your account, your
posts, or your social graph but you. Your node is your data, and because no
server keeps a copy for you, you are responsible for it. Back it up, hold
your own keys, and keep your node online to stay reachable. Lose the key and
the identity is gone, with no recovery and no one to ask.

### Identity

Your identity is `driftnode:<base32-pubkey>`. There is no username, no email, no
hostname baked in. The private key signs every event you publish. Losing the
key means losing the identity; there is no account recovery. Back it up:

```sh
driftnode --db ~/.driftnode/node.db backup export -o my-account.cbor -p "your-passphrase"
# Store this file safely. It is your account.
```

Restore or migrate to a new device:

```sh
driftnode --db ~/.driftnode/node.db backup import my-account.cbor -p "your-passphrase"
```

### Event logs

Each identity has three append-only logs of signed events:

- **Profile log**: your zen name, avatar, and follow/unfollow edges. Small,
  public, fetched and durably cached by the crawler. Anyone can request it
  without touching your posts.
- **Post log**: posts, replies, likes, deletes. Only synced with zens who
  follow you.
- **Detail log**: bio, first/last name, location. Never crawled or cached by
  peers; a zen fetches it on demand to show your full profile and discards it
  afterwards.

Events are immutable and individually signed. Merging two zens' views is a
set union, not a conflict resolution. This is the same model as Nostr and
Secure Scuttlebutt.

### Sync

When two zens connect, each pulls the other's profile log and post log (if
they follow each other), exchanges known zen tokens, and serves its own logs
back over the same connection. A timestamp cursor tracks what has already been
synced per zen per log. Sync is idempotent: re-syncing a zen you have already
synced with merges zero new events.

### Discovery

Three mechanisms, in order of how much infrastructure they need:

1. **Manual connection**: share your `driftnode:` identity string directly
   with someone. `driftnode follow <pubkey>` dials the node (resolving its
   current address token automatically) and adds a follow edge in one gesture.
2. **Bootstrap file**: a signed `bootstrap.yaml` lists seed zens and crawl
   seeds. A fresh node fetches it, verifies the signature, auto-dials the
   seeds, and starts crawling the follow graph from the crawl seeds.
3. **Crawler**: every daemon walks the public follow graph in the background,
   fetching only profile logs (never post logs). A node that has been online
   longer accumulates a larger crawl cache as a side effect of participating.

Zen exchange extends discovery: when you sync with a zen, each side shares
its known zen tokens. A node that bootstraps a single seed discovers the rest
of the network through this exchange, without any central directory.

### The daemon

The daemon is a long-running process that holds network state: Tailcat
tunnels, the crawl cache, active zen connections. Network commands (`sync`,
`zens`, `bootstrap`) talk to it over a local control socket. Signing and read
commands (`post`, `follow`, `whoami`, `feed`) prefer the daemon when it is
running, and fall back to opening the store directly when it isn't (passing
`-p` to unlock the key).

The daemon keeps your signing key locked at rest. Unlock it once after
starting so the commands above don't re-supply the passphrase on every call:

```sh
driftnode --db ~/.driftnode/node.db daemon            # start in foreground (logs to stdout)
driftnode --db ~/.driftnode/node.db daemon -d         # start in background (logs to a file)
driftnode --db ~/.driftnode/node.db daemon restart    # stop and start a fresh daemon (detached)
driftnode --db ~/.driftnode/node.db daemon unlock    # hold the key in memory
driftnode --db ~/.driftnode/node.db daemon lock       # clear the in-memory key
driftnode --db ~/.driftnode/node.db daemon stop       # stop via control socket
driftnode --db ~/.driftnode/node.db daemon status     # running, zens, transport, unlocked
```

### The TUI

```sh
driftnode --db ~/.driftnode/node.db tui
```

A tabbed terminal dashboard: **Feed** (your merged timeline), **Zens**
(discovered and connected zens), **Follows** (who you follow), and
**Followers** (who follows you), with a persistent compose line. The TUI is a
thin client of the daemon's control socket (which must be running and
unlocked), so it never opens the store itself. Keys: `Tab`/`Shift+Tab` switch
tabs, `Enter` posts, `Esc` clears a half-typed post (or quits when the line is
empty), `Ctrl+C` quits. On the Zens tab, `i`/`f`/`u` act on the selected zen
(info, follow, unfollow).

## Command reference

| Command | Purpose |
|---|---|
| `init -p <passphrase>` | Generate an Ed25519 identity |
| `whoami` | Print your identity, address token, zen name, and detail fields |
| `post [-p <passphrase>] "<text>"` | Append a signed post |
| `feed [--limit N]` | Print your merged timeline |
| `follow [-p <passphrase>] <pubkey>` | Follow an identity (dials and syncs) |
| `unfollow [-p <passphrase>] <pubkey>` | Unfollow an identity |
| `follows` | List the identities this zen follows |
| `followers` | List the identities that follow this zen |
| `profile [-p <passphrase>] --name <name>` | Set your public zen name |
| `detail [-p <passphrase>] [--bio \| --first-name \| --last-name \| --location <v>]` | Set personal metadata shown only on direct request |
| `daemon [-d]` | Start the networking daemon (default foreground; `-d` to run in background) |
| `daemon restart` | Stop the running daemon and start a fresh one (detached) |
| `daemon unlock [-p <passphrase>]` | Unlock the daemon's signing key |
| `daemon lock` | Clear the daemon's in-memory signing key |
| `daemon stop` | Stop the daemon |
| `daemon status` | Show running, zens, transport, and unlocked state |
| `sync` | Trigger a sync round |
| `rotate-key` | Generate a fresh address token, persist it, and restart the listener |
| `zens list` | Show connected zens (identity, name, verified status) |
| `zens verify <identity>` | Mark an identity confirmed out-of-band |
| `zens unverify <identity>` | Remove an out-of-band confirmation |
| `bootstrap keygen [--key-out <path> \| --key <path>]` | Generate a bootstrap signing keypair, or derive the public key from an existing private key |
| `bootstrap sign <file> --key <keyfile>` | Sign a bootstrap.yaml |
| `bootstrap verify <file> [--key <pubkey-file>]` | Verify a bootstrap.yaml against a base64 public key file |
| `backup export [-o <path>] [-p <passphrase>]` | Export your account (including the transport key) to a single file |
| `backup import <path> [-p <passphrase>]` | Restore from a backup file |
| `key export [-o <path>]` | Export your encrypted keypair |
| `key import <path> -p <passphrase>` | Import an encrypted keypair |
| `tui` | Launch the terminal UI |
| `relay status` | Check relay/bootstrap role |
| `relay enable` | Opt in to advertising as a relay candidate (not yet implemented) |
| `relay disable` | Opt out of advertising as a relay candidate (not yet implemented) |

For signing commands, `-p <passphrase>` is optional when the daemon is
unlocked and required when it is not.

Daemon flags:

| Flag | Purpose |
|---|---|
| `--ephemeral` | Generate a fresh address token each run (do not persist the transport key) |
| `--bootstrap <path>` | Load a signed bootstrap.yaml and auto-dial its seed zens |
| `--bootstrap-key <path>` | File containing the base64 Ed25519 public key that signed the bootstrap |
| `--idle-lock <duration>` | Auto-lock the signing key after this idle period (e.g. `5m`, `1h`); default keeps it unlocked until `daemon lock` or stop |
| `-d`, `--detached` | Run in the background with logs to a file (default: foreground with logs on stdout) |

## Data location

The local store (bbolt) path is set with the required `--db` flag. Each store
gets its own control socket, derived from the store path, so multiple daemons
on the same machine don't collide.

```sh
driftnode --db ~/.driftnode/node.db init -p "your-passphrase"
```

## Testing

### Unit tests

```sh
go test ./...
```

Covers identity, canonical encoding, signing, event/log model, store CRUD,
followed-event dedup, wire protocol, bidirectional session, zen exchange,
bootstrap sign/verify/tamper detection, crawler BFS, and daemon RPC.

### Containerized end-to-end test

```sh
bash scripts/test-containers.sh
```

Spins up five isolated Docker containers (two regional seeds, three fresh
users) and exercises the full discovery, sync, and feed propagation chain over
real Tailcat tunnels. Requires Docker or OrbStack.

## Design document

The full design rationale, architecture, and tradeoffs are in
[`serverless-social-network-design.md`](serverless-social-network-design.md).

If you want to run a public seed node so others can find the network, see
[`BOOTSTRAP.md`](BOOTSTRAP.md).

## License

MIT
