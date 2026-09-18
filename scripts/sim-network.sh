#!/usr/bin/env bash
# sim-network.sh - Stand up a persistent driftnode social network in
# isolated Docker containers, then leave it running for manual use.
#
# Same five-node topology as test-containers.sh (two regional seeds, three
# fresh users that bootstrap off them), but the daemons keep running after
# setup so the network behaves like a live one: posts keep propagating,
# peers stay connected, the crawler keeps walking the follow graph. Daemon
# logs are copied to sim-logs/ on the host for easy retrieval.
#
# Usage:
#   bash scripts/sim-network.sh          start the network (refresh if up)
#   bash scripts/sim-network.sh --down   stop and clean up
#
# External fresh nodes can join a running network. The script signs the
# bootstrap files with a generated Ed25519 key and publishes them, with the
# matching public key, to sim-logs/public/. A fresh node verifies the
# signature by starting its daemon with both flags:
#   driftnode daemon --bootstrap bootstrap-eu.yaml --bootstrap-key <b64-pubkey>
#
# Requires Docker (or OrbStack) with AF_ROUTE support in the containers.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$REPO_ROOT/sim-logs"
PUB_DIR="$LOG_DIR/public"          # signed bootstrap files + verify key for external nodes
KEY_DIR="$REPO_ROOT/sim-keys"      # host-side signing key (gitignored)
NODES="seed-eu seed-us carol dave eve"
cd "$REPO_ROOT"

# Run a driftnode command inside a container, capture stdout+stderr.
dn()  { docker exec "$1" driftnode --db /data/node.db "${@:2}" 2>&1; }
# Run a driftnode command inside a container, suppress output, return exit code.
dnq() { docker exec "$1" driftnode --db /data/node.db "${@:2}" >/dev/null 2>&1; }

# Start the daemon inside a container, logging to /data/daemon.log.
start_daemon() {
  local container="$1"; shift
  docker exec -d "$container" sh -c "driftnode --db /data/node.db daemon --foreground $* > /data/daemon.log 2>&1"
}

# Unlock a node's signing key so post/follow omit the passphrase. Run after
# start_daemon; the daemon must be up for the unlock RPC to connect.
unlock_node() { docker exec "$1" driftnode --db /data/node.db daemon unlock -p "$2" >/dev/null 2>&1; }

# Copy each node's daemon log to sim-logs/<node>.log on the host.
collect_logs() {
  mkdir -p "$LOG_DIR"
  for c in $NODES; do
    docker cp "$c:/data/daemon.log" "$LOG_DIR/$c.log" 2>/dev/null \
      || printf "(no daemon log for %s yet)\n" "$c" > "$LOG_DIR/$c.log"
  done
}

ok()    { printf "  %-50s ok\n" "$1"; }
warn()  { printf "  %-50s WARN: %s\n" "$1" "$2"; }
fatal() { printf "FATAL: %s\n" "$1"; exit 1; }

# True if the container exists and is running.
is_running() { docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null | grep -q true; }

teardown() {
  printf "Saving daemon logs to %s ...\n" "$LOG_DIR"
  collect_logs
  printf "Stopping containers...\n"
  docker compose down --remove-orphans --volumes 2>/dev/null || true
  rm -f "$REPO_ROOT/bootstrap-eu.yaml" "$REPO_ROOT/bootstrap-us.yaml"
  printf "Network stopped. Logs retained in %s\n" "$LOG_DIR"
}

print_status() {
  printf "\n===== Network status =====\n"
  for c in $NODES; do
    local id token peers
    id=$(dn "$c" whoami 2>/dev/null | head -1)
    token=$(dn "$c" peers token 2>/dev/null | head -1)
    peers=$(dn "$c" peers list 2>/dev/null | grep -vc 'no peers connected')
    printf "  %s\n" "$c"
    printf "    id:    %s\n" "${id:-none}"
    printf "    token: %s\n" "${token:-none}"
    printf "    peers: %s connected\n" "$peers"
  done

  printf "\n----- carol's feed (first 10) -----\n"
  dn carol feed 2>/dev/null | head -10 || true
  printf "----- end -----\n"

  printf "\nInteract with a node (example):\n"
  printf "  docker exec -it carol driftnode --db /data/node.db daemon unlock -p carolpass\n"
  printf "  docker exec -it carol driftnode --db /data/node.db feed\n"
  printf "  docker exec -it carol driftnode --db /data/node.db post \"hi\"\n"
  printf "\nFollow a node's live daemon log:\n"
  printf "  docker exec carol tail -f /data/daemon.log\n"
  printf "  docker compose logs -f\n"

  printf "\n----- Join with a fresh external node -----\n"
  printf "Signed bootstrap files and the verify key are published to:\n"
  printf "  %s/bootstrap-eu.yaml\n" "$PUB_DIR"
  printf "  %s/bootstrap-us.yaml\n" "$PUB_DIR"
  printf "  %s/bootstrap-pubkey.b64   (base64 Ed25519 public key)\n" "$PUB_DIR"
  printf "\nOn any host with the driftnode binary and a route to a seed token:\n"
  printf "  driftnode --db <path> init -p <pass>\n"
  printf "  driftnode --db <path> daemon --bootstrap bootstrap-eu.yaml \\\n"
  printf "    --bootstrap-key bootstrap-pubkey.b64\n"
  printf "The daemon verifies the bootstrap signature, auto-dials the seed, and\n"
  printf "joins the network through peer exchange and crawling.\n"

  printf "\nDaemon logs saved to: %s\n" "$LOG_DIR"
  printf "Stop the network:     bash scripts/sim-network.sh --down\n"
  printf "\nNetwork is up and running.\n"
}

# Load the published verify key if a previous run published one, so
# print_status can show the join instructions on a refresh (no re-provision).
BOOTSTRAP_PUB_B64=""
[[ -s "$PUB_DIR/bootstrap-pubkey.b64" ]] && BOOTSTRAP_PUB_B64=$(tr -d '\n' < "$PUB_DIR/bootstrap-pubkey.b64")

# --- mode dispatch ---

if [[ "${1:-}" == "--down" ]]; then
  teardown
  exit 0
fi

# If the seed-eu daemon is already running, the network is up: refresh
# logs and report instead of re-provisioning. (daemon status prints
# "not running" but still exits 0, so parse the output directly.)
if is_running seed-eu && dn seed-eu daemon status 2>/dev/null | grep -q '^running: true'; then
  printf "Network already running. Refreshing logs...\n"
  collect_logs
  print_status
  exit 0
fi

# --- fresh setup ---

printf "Resetting to a clean state...\n"
docker compose down --remove-orphans --volumes 2>/dev/null || true
rm -f "$REPO_ROOT/bootstrap-eu.yaml" "$REPO_ROOT/bootstrap-us.yaml"

# Placeholder bootstrap files so the bind mounts exist at container start.
# Real content is written after seed tokens are known.
touch "$REPO_ROOT/bootstrap-eu.yaml" "$REPO_ROOT/bootstrap-us.yaml"

printf "Building Docker image...\n"
docker compose build --quiet || { printf "BUILD FAILED\n"; exit 1; }

printf "Starting containers...\n"
docker compose up -d --quiet-pull
sleep 2

printf "\n===== Seeds =====\n"
SEED_EU_ID=$(dn seed-eu init -p seedpass)
SEED_US_ID=$(dn seed-us init -p seedpass)
[[ "$SEED_EU_ID" == driftnode:* ]] || fatal "seed-eu init failed: $SEED_EU_ID"
[[ "$SEED_US_ID" == driftnode:* ]] || fatal "seed-us init failed: $SEED_US_ID"
ok "seeds initialized"

dnq seed-eu follow -p seedpass "$SEED_US_ID" && ok "seed-eu follows seed-us" || warn "seed-eu follow" "failed"
dnq seed-us follow -p seedpass "$SEED_EU_ID" && ok "seed-us follows seed-eu" || warn "seed-us follow" "failed"

dnq seed-eu post -p seedpass "hello from eu" && ok "seed-eu posts" || warn "seed-eu post" "failed"
dnq seed-us post -p seedpass "hello from us" && ok "seed-us posts" || warn "seed-us post" "failed"

start_daemon seed-eu --key /data/tc.key
start_daemon seed-us --key /data/tc.key
sleep 5

SEED_EU_TOKEN=$(dn seed-eu peers token)
SEED_US_TOKEN=$(dn seed-us peers token)
[[ "$SEED_EU_TOKEN" == tc* ]] || fatal "seed-eu token: $SEED_EU_TOKEN"
[[ "$SEED_US_TOKEN" == tc* ]] || fatal "seed-us token: $SEED_US_TOKEN"
ok "seed tokens ready"

dnq seed-eu peers add "$SEED_US_TOKEN" && ok "seed-eu dials seed-us" || warn "seed-eu dial" "failed"
dnq seed-us peers add "$SEED_EU_TOKEN" && ok "seed-us dials seed-eu" || warn "seed-us dial" "failed"
sleep 5

printf "\n===== Bootstrap signing key =====\n"
# Generate a raw Ed25519 keypair for signing the bootstrap files. The
# private key (64 bytes) is what `driftnode bootstrap sign --key` reads;
# the public key (base64) is what `--bootstrap-key` verifies. Both are
# produced by `driftnode bootstrap keygen` and persisted under sim-keys/
# so the signature stays stable when the sim is rebuilt.
mkdir -p "$KEY_DIR" "$PUB_DIR"
if [[ ! -s "$KEY_DIR/bootstrap-signing.key" ]]; then
  # keygen prints the base64 public key to stdout and writes the raw
  # 64-byte private key to --key-out.
  BOOTSTRAP_PUB_B64=$(docker exec seed-eu driftnode bootstrap keygen --key-out /tmp/bs-sign.key | tr -d '\n')
  docker cp seed-eu:/tmp/bs-sign.key "$KEY_DIR/bootstrap-signing.key"
  ok "generated bootstrap signing key"
else
  docker cp "$KEY_DIR/bootstrap-signing.key" seed-eu:/tmp/bs-sign.key
  # Derive the public key from the existing private key.
  BOOTSTRAP_PUB_B64=$(docker exec seed-eu driftnode bootstrap keygen --key /tmp/bs-sign.key | tr -d '\n')
  ok "reusing existing bootstrap signing key"
fi
ok "bootstrap verify key: $BOOTSTRAP_PUB_B64"

printf "\n===== Bootstrap files =====\n"
cat > "$REPO_ROOT/bootstrap-eu.yaml" <<EOF
version: 1
signature: ""
seed_peers:
  - token: "$SEED_EU_TOKEN"
    kind: native_peer
crawl_seeds:
  - "$SEED_EU_ID"
  - "$SEED_US_ID"
EOF
cat > "$REPO_ROOT/bootstrap-us.yaml" <<EOF
version: 1
signature: ""
seed_peers:
  - token: "$SEED_US_TOKEN"
    kind: native_peer
crawl_seeds:
  - "$SEED_EU_ID"
  - "$SEED_US_ID"
EOF
ok "bootstrap files written"

# Sign both bootstrap files with the generated private key. The signer
# runs in a container but reads the key from a bind mount.
docker cp "$KEY_DIR/bootstrap-signing.key" seed-eu:/tmp/bs-sign.key
for f in bootstrap-eu bootstrap-us; do
  docker cp "$REPO_ROOT/$f.yaml" seed-eu:/tmp/$f.yaml
  docker exec seed-eu driftnode bootstrap sign /tmp/$f.yaml --key /tmp/bs-sign.key >/dev/null 2>&1 \
    || fatal "sign $f.yaml failed"
  docker cp seed-eu:/tmp/$f.yaml "$REPO_ROOT/$f.yaml"
done
ok "bootstrap files signed"

# Publish signed bootstrap files and the verify key for external fresh nodes.
cp "$REPO_ROOT/bootstrap-eu.yaml" "$PUB_DIR/bootstrap-eu.yaml"
cp "$REPO_ROOT/bootstrap-us.yaml" "$PUB_DIR/bootstrap-us.yaml"
printf '%s\n' "$BOOTSTRAP_PUB_B64" > "$PUB_DIR/bootstrap-pubkey.b64"
ok "published to $PUB_DIR"

# Self-check: verify the published host file against the published key.
# --bootstrap-key and verify --key read the base64 public key from a file,
# so copy the published key file into the container.
docker cp "$PUB_DIR/bootstrap-pubkey.b64" seed-eu:/tmp/bs-pubkey.b64
docker cp "$PUB_DIR/bootstrap-eu.yaml" seed-eu:/tmp/bs-verify.yaml
VERIFY_OUT=$(docker exec seed-eu driftnode bootstrap verify /tmp/bs-verify.yaml --key /tmp/bs-pubkey.b64 2>&1)
echo "$VERIFY_OUT" | grep -q "signature: verified" \
  && ok "published bootstrap-eu signature verifies" \
  || fatal "published bootstrap-eu verify failed: $VERIFY_OUT"

printf "\n===== Fresh nodes =====\n"
CAROL_ID=$(dn carol init -p carolpass)
DAVE_ID=$(dn dave init -p davepass)
EVE_ID=$(dn eve init -p evepass)
[[ "$CAROL_ID" == driftnode:* ]] || fatal "carol init: $CAROL_ID"
[[ "$DAVE_ID" == driftnode:* ]] || fatal "dave init: $DAVE_ID"
[[ "$EVE_ID" == driftnode:* ]] || fatal "eve init: $EVE_ID"
ok "fresh nodes initialized"

# Copy the verify key file into each fresh node so the daemon can read it
# via --bootstrap-key (which reads the base64 public key from a file).
for c in carol dave eve; do
  docker cp "$PUB_DIR/bootstrap-pubkey.b64" "$c:/data/bs-pubkey.b64"
done

start_daemon carol --bootstrap /bootstrap/bootstrap.yaml --bootstrap-key /data/bs-pubkey.b64
start_daemon dave --bootstrap /bootstrap/bootstrap.yaml --bootstrap-key /data/bs-pubkey.b64
start_daemon eve --bootstrap /bootstrap/bootstrap.yaml --bootstrap-key /data/bs-pubkey.b64
sleep 10
unlock_node carol carolpass
unlock_node dave davepass
unlock_node eve evepass
ok "fresh nodes bootstrapped"

printf "\n===== Cross-seed discovery =====\n"
dnq carol sync --now
dnq dave sync --now
sleep 15

printf "\n===== Building social graph =====\n"
dnq dave follow "$CAROL_ID"
dnq carol post "carol's first post"
dnq dave sync --now
sleep 5

dnq carol follow "$DAVE_ID"
dnq dave post "dave posts here"
dnq carol sync --now
sleep 5

dnq eve post "eve checking in"
dnq eve follow "$CAROL_ID"
dnq eve sync --now
sleep 5
dnq carol follow "$EVE_ID"
dnq carol sync --now
sleep 5

dnq carol post "carol late post"
dnq dave sync --now
sleep 5
ok "social graph established"

printf "\n===== Collecting logs =====\n"
collect_logs
print_status
