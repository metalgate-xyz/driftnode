# Running a driftnode Bootstrap Node

A bootstrap node is a `driftnode` that has a stable network address and is
listed in a signed `bootstrap.yaml` so that fresh installs can find their first
peers. The network needs a handful of these across regions. Anyone can run
one.

Running a bootstrap node does not give you any special privilege. A bootstrap
node holds no data it did not already choose to publish. It is a routing entry
point, not a server with user data.

## Why volunteer

A fresh `driftnode` install knows no peers. It fetches a signed
`bootstrap.yaml`, dials the seed peers listed there, and joins the network
through them. Without bootstrap nodes, new users have no way in.

The more bootstrap nodes there are, across more regions and networks, the
more resilient cold-start discovery is. Three to five nodes in different
geographic regions is enough for a healthy network.

## What you need

- A machine that stays online and connected: a VPS, a home server, or a
  always-on box. It does not need a static IP or a forwarded port; Tailcat's
  DERP relay handles NAT traversal automatically. A cheap VPS (512MB RAM,
  single core) is sufficient.
- Docker, or the ability to build a Go binary.
- The `driftnode` binary.

## Step 1: Generate a persistent key

A bootstrap node needs a stable Tailcat address token so that its entry in
`bootstrap.yaml` remains valid across restarts. The token is derived from a
Tailcat keypair, not your IP address, so it survives network changes as long
as the key file is preserved. Without a persistent key, the token changes
every time the daemon restarts.

```sh
driftnode init -p "your-passphrase"
```

Start the daemon with the `--key` flag to persist the Tailcat key:

```sh
driftnode daemon --foreground --key /var/lib/driftnode/tc.key
```

The key file is created on first run and reused on subsequent restarts. The
address token it produces stays the same.

## Step 2: Get your address token

With the daemon running:

```sh
driftnode peers token
```

This prints a `tc...` string. That is your node's dialable address. Peers and
fresh installs use it to connect to you.

## Step 3: Verify your token is stable

Restart the daemon and check the token again:

```sh
driftnode daemon stop
driftnode daemon --foreground --key /var/lib/driftnode/tc.key
driftnode peers token
```

The token should be identical. If it changed, the key file path is wrong or
inaccessible.

## Step 4: Post and follow

A bootstrap node is a normal node. Create some content and follow the other
seeds so the genesis graph is not empty:

```sh
driftnode post -p "your-passphrase" "seed node online in EU"
driftnode follow -p "your-passphrase" driftnode:<other-seed-pubkey>
```

Fresh nodes crawl the follow graph starting from `crawl_seeds` in
`bootstrap.yaml`. If the seeds follow each other and have posts, new users see
real content on their first sync.

## Step 5: Create and sign a bootstrap.yaml

A `bootstrap.yaml` is a static YAML file listing seed peers and crawl seeds.
It is signed with an Ed25519 key so that any mirror serving it can be verified.

```yaml
version: 1
signature: ""
seed_peers:
  - token: "tc...your-token..."
    kind: native_peer
crawl_seeds:
  - "driftnode:your-pubkey"
  - "driftnode:other-seed-pubkey"
```

Sign it with a private key (a 64-byte raw Ed25519 key file):

```sh
driftnode bootstrap sign bootstrap.yaml --key ed25519-private.key
```

Anyone can verify it against the corresponding public key:

```sh
driftnode bootstrap verify bootstrap.yaml --key <base64-public-key>
```

Distribute the signed file. It can be served from GitHub raw, an IPFS gateway,
a static site, or any mirror. The signature is what guarantees authenticity,
not the transport.

## Step 6: Run the daemon persistently

For a production bootstrap node, run the daemon under a process supervisor
(systemd, launchd, Docker restart policy) so it restarts after crashes or
reboots. The `--key` flag must be passed every time to keep the token stable.

### systemd example

```ini
[Unit]
Description=driftnode bootstrap node
After=network.target

[Service]
ExecStart=/usr/local/bin/driftnode daemon --foreground --key /var/lib/driftnode/tc.key
Restart=always
User=driftnode
StateDirectory=driftnode

[Install]
WantedBy=multi-user.target
```

### Docker example

```sh
docker run -d \
  --name driftnode-seed \
  --cap-add=NET_ADMIN \
  -v driftnode-data:/data \
  driftnode driftnode --db /data/node.db daemon --foreground --key /data/tc.key
```

The `--cap-add=NET_ADMIN` flag is required for Tailcat's network monitor.

## Keeping the token stable

The single most important property of a bootstrap node is that its address
token does not change. The `--key` flag persists the Tailcat keypair to a file
and reuses it across restarts. If the key file is lost or deleted, the node
generates a new one on next start and gets a new token. The old entry in
`bootstrap.yaml` becomes unreachable and must be updated.

Back up the key file the same way you back up your identity:

```sh
cp /var/lib/driftnode/tc.key tc.key.backup
# Store tc.key.backup somewhere safe.
```

## Updating bootstrap.yaml

As the network grows, `crawl_seeds` should evolve. Periodically re-run your
daemon's crawl and select high-in-degree accounts as new crawl seeds. Sign and
publish a new version. Old versions keep working; the file is a cold-start
convenience, not an ongoing dependency. Once a node has crawled once, it no
longer needs `crawl_seeds`.

## Firewall and network notes

- Tailcat uses WireGuard. It attempts UDP hole-punching and falls back to a
  DERP relay if that fails. No port forwarding is required; a node behind NAT
  or a changing IP works fine. A node with a public IP and direct UDP
  reachability provides lower latency for peers dialing in, but it is not a
  prerequisite.
- The `--cap-add=NET_ADMIN` Docker flag is needed for Tailcat's network
  monitor. OrbStack provides this. On Linux, the container runtime grants it.

## Resource usage

A bootstrap node is lightweight:

- RAM: under 512MB for a small network. The crawl cache is bounded and
  profile logs are small.
- Disk: grows with the number of followed post logs and the crawl cache. A
  `postlog_retention_days` config cap is available if you want to limit
  history. Crawled profile logs are bounded by an LRU cache.
- Bandwidth: proportional to the number of peers syncing with you. Each sync
  round transfers only new events since the last sync.

## Trust and what a bootstrap node can and cannot do

A bootstrap node:

- Can see the public profile logs and post logs of peers it syncs with
  (these are already public, signed content).
- Can relay routing information (peer tokens) so new nodes discover each other.
- Cannot forge posts or follows (every event is signed by its author's key).
- Cannot read private messages (there are no private messages in v1).
- Cannot censor or rank anyone's feed (feed construction is entirely
  client-side).

The `bootstrap.yaml` signing key is the only real influence a bootstrap
operator has: it determines what a brand-new user sees first. It cannot touch
anyone's data after that user's first crawl. Anyone can fork the bootstrap
file, sign their own copy, and run their own seed set.
