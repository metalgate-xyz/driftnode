#!/usr/bin/env bash
# test-containers.sh - Realistic end-to-end test of driftnode Phase 0 in
# isolated Docker containers, managed via docker compose.
#
# Five nodes (two seeds in different regions, three fresh users) discover
# each other through bootstrap, crawling, and zen exchange, then sync feeds
# over real tailcat tunnels.
#
# Discovery chain:
#   seed-eu, seed-us: genesis zens, follow each other, post.
#   carol: bootstraps seed-eu only. Discovers seed-us through zen exchange
#         (seed-eu relays seed-us's token) and the crawler (walks seed-eu's
#         follow graph). NOT through a direct bootstrap dial.
#   dave: bootstraps seed-us only. Discovers seed-eu the same way.
#   eve: bootstraps seed-eu. Discovers others through zen exchange + crawl.
#   dave discovers carol via zen exchange (seed-eu relays carol's token).
#   dave follows carol. carol follows dave. Both see each other's posts.
#
# Requires Docker (or OrbStack) and AF_ROUTE support in the containers.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

PASS=0; FAIL=0
ok()  { printf "  %-55s PASS\n" "$1"; PASS=$((PASS+1)); }
bad() { printf "  %-55s FAIL: %s\n" "$1" "$2"; FAIL=$((FAIL+1)); }
contains() { echo "$1" | grep -qF "$2"; }

cleanup() {
  printf "\n===== DAEMON LOGS =====\n"
  for c in seed-eu seed-us carol dave eve; do dumplog "$c"; done
  docker compose down --remove-orphans --volumes 2>/dev/null || true
  rm -f "$REPO_ROOT/bootstrap-eu.yaml" "$REPO_ROOT/bootstrap-us.yaml"
}
trap cleanup EXIT

# Run a driftnode command inside a container, capture stdout+stderr.
dn()  { docker exec "$1" driftnode --db /data/node.db "${@:2}" 2>&1; }
# Run a driftnode command inside a container, suppress output, return exit code.
dnq() { docker exec "$1" driftnode --db /data/node.db "${@:2}" >/dev/null 2>&1; }

# Dump a container's daemon log.
dumplog() { echo "--- $1 daemon.log ---"; docker exec "$1" cat /data/daemon.log 2>/dev/null || echo "(no log)"; }

# Start the daemon inside a container, logging to /data/daemon.log.
start_daemon() {
  local container="$1"; shift
  docker exec -d "$container" sh -c "driftnode --db /data/node.db daemon --foreground $* > /data/daemon.log 2>&1"
}

# Unlock a node's signing key so post/follow omit the passphrase. Run after
# start_daemon; the daemon must be up for the unlock RPC to connect.
unlock_node() { docker exec "$1" driftnode --db /data/node.db daemon unlock -p "$2" >/dev/null 2>&1; }

# Create placeholder bootstrap files so the bind mounts exist at container
# start. Real content is written after seed tokens are known.
touch "$REPO_ROOT/bootstrap-eu.yaml" "$REPO_ROOT/bootstrap-us.yaml"

printf "Building Docker image...\n"
docker compose build --quiet || { printf "BUILD FAILED\n"; exit 1; }

printf "Starting containers...\n"
docker compose up -d --quiet-pull
sleep 5

printf "\n===== Phase 0: Seed initialization =====\n"
SEED_EU_ID=$(dn seed-eu init -p seedpass)
SEED_US_ID=$(dn seed-us init -p seedpass)
[[ "$SEED_EU_ID" == driftnode:* ]] && ok "S1 seed-eu init" || bad "S1 seed-eu init" "$SEED_EU_ID"
[[ "$SEED_US_ID" == driftnode:* ]] && ok "S1 seed-us init" || bad "S1 seed-us init" "$SEED_US_ID"

dnq seed-eu follow -p seedpass "$SEED_US_ID" && ok "S2 seed-eu follows seed-us" || bad "S2 seed-eu follows seed-us" "failed"
dnq seed-us follow -p seedpass "$SEED_EU_ID" && ok "S2 seed-us follows seed-eu" || bad "S2 seed-us follows seed-eu" "failed"

dnq seed-eu post -p seedpass "hello from eu" && ok "S3 seed-eu posts" || bad "S3 seed-eu posts" "failed"
dnq seed-us post -p seedpass "hello from us" && ok "S3 seed-us posts" || bad "S3 seed-us posts" "failed"

# Start the seed daemons, then unlock so follow-by-token can sign and
# inbound sessions can authenticate.
start_daemon seed-eu
start_daemon seed-us
sleep 5
unlock_node seed-eu seedpass
unlock_node seed-us seedpass

SEED_EU_TOKEN=$(dn seed-eu whoami | grep '^address:' | awk '{print $2}')
SEED_US_TOKEN=$(dn seed-us whoami | grep '^address:' | awk '{print $2}')
[[ "$SEED_EU_TOKEN" == tc* ]] && ok "S4 seed-eu token" || bad "S4 seed-eu token" "$SEED_EU_TOKEN"
[[ "$SEED_US_TOKEN" == tc* ]] && ok "S4 seed-us token" || bad "S4 seed-us token" "$SEED_US_TOKEN"

dnq seed-eu follow -p seedpass "$SEED_US_TOKEN" && ok "S5 seed-eu dials seed-us" || bad "S5 seed-eu dials seed-us" "failed"
dnq seed-us follow -p seedpass "$SEED_EU_TOKEN" && ok "S5 seed-us dials seed-eu" || bad "S5 seed-us dials seed-eu" "failed"
sleep 5

printf "\n===== Phase 0.5: Bootstrap files =====\n"
cat > "$REPO_ROOT/bootstrap-eu.yaml" <<EOF
version: 1
signature: ""
seed_zens:
  - token: "$SEED_EU_TOKEN"
    kind: native_zen
crawl_seeds:
  - "$SEED_EU_ID"
  - "$SEED_US_ID"
EOF
cat > "$REPO_ROOT/bootstrap-us.yaml" <<EOF
version: 1
signature: ""
seed_zens:
  - token: "$SEED_US_TOKEN"
    kind: native_zen
crawl_seeds:
  - "$SEED_EU_ID"
  - "$SEED_US_ID"
EOF
ok "B1 bootstrap files created"
printf "\n===== Phase 1: Fresh nodes bootstrap =====\n"
CAROL_ID=$(dn carol init -p carolpass)
DAVE_ID=$(dn dave init -p davepass)
EVE_ID=$(dn eve init -p evepass)
[[ "$CAROL_ID" == driftnode:* ]] && ok "F1 carol init" || bad "F1 carol init" "$CAROL_ID"
[[ "$DAVE_ID" == driftnode:* ]] && ok "F1 dave init" || bad "F1 dave init" "$DAVE_ID"
[[ "$EVE_ID" == driftnode:* ]] && ok "F1 eve init" || bad "F1 eve init" "$EVE_ID"

start_daemon carol --bootstrap /bootstrap/bootstrap.yaml
start_daemon dave --bootstrap /bootstrap/bootstrap.yaml
start_daemon eve --bootstrap /bootstrap/bootstrap.yaml
sleep 2
# Unlock immediately: the daemon retries bootstrap auto-follow on unlock,
# so seeds enter the follow graph even though the key was locked at start.
unlock_node carol carolpass
unlock_node dave davepass
unlock_node eve evepass
sleep 10

CAROL_FEED=$(dn carol feed)
contains "$CAROL_FEED" "hello from eu" && ok "P1.1 carol sees seed-eu post (bootstrap auto-dial)" || bad "P1.1 carol sees seed-eu" "$CAROL_FEED"

DAVE_FEED=$(dn dave feed)
contains "$DAVE_FEED" "hello from us" && ok "P1.2 dave sees seed-us post (bootstrap auto-dial)" || bad "P1.2 dave sees seed-us" "$DAVE_FEED"

EVE_FEED=$(dn eve feed)
contains "$EVE_FEED" "hello from eu" && ok "P1.3 eve sees seed-eu post (bootstrap auto-dial)" || bad "P1.3 eve sees seed-eu" "$EVE_FEED"

printf "\n===== Phase 1.5: Out-of-band identity verification =====\n"
# Each node confirms every other node's identity (§7): the operator is
# assumed to have compared the full driftnode:<pubkey> strings out of band
# (here, all nodes are in the same trust domain, so all are verified).
NODES_V=(seed-eu seed-us carol dave eve)
IDS_V=("$SEED_EU_ID" "$SEED_US_ID" "$CAROL_ID" "$DAVE_ID" "$EVE_ID")
for i in "${!NODES_V[@]}"; do
  for j in "${!IDS_V[@]}"; do
    [[ "$i" -eq "$j" ]] && continue
    dnq "${NODES_V[i]}" zens verify "${IDS_V[j]}" \
      && ok "${NODES_V[i]} verifies ${NODES_V[j]}" \
      || bad "${NODES_V[i]} verifies ${NODES_V[j]}" "failed"
  done
done

printf "\n===== Phase 2: Cross-seed discovery (zen exchange + crawl) =====\n"
# Carol bootstrapped only seed-eu. She discovers seed-us through zen exchange
# (seed-eu relays seed-us's token during sync) and the crawler (walks seed-eu's
# follow graph and fetches seed-us's ProfileLog). Discovery alone does not
# dial: carol must follow seed-us to enter it into her follow graph, then
# sync again to pull its posts.
dnq carol sync
dnq dave sync
sleep 10
# Now carol/dave have learned the cross-seed identity (via crawl) and token
# (via zen exchange). Follow by identity; the follow auto-triggers a sync
# that resolves the pending follow through a known token and pulls posts.
dnq carol follow "$SEED_US_ID"
dnq dave follow "$SEED_EU_ID"
sleep 10

CAROL_FEED2=$(dn carol feed)
contains "$CAROL_FEED2" "hello from us" && ok "P2.1 carol discovers seed-us (not in her bootstrap)" || bad "P2.1 carol discovers seed-us" "$CAROL_FEED2"

DAVE_FEED2=$(dn dave feed)
contains "$DAVE_FEED2" "hello from eu" && ok "P2.2 dave discovers seed-eu (not in his bootstrap)" || bad "P2.2 dave discovers seed-eu" "$DAVE_FEED2"

printf "\n===== Phase 3: Zen exchange between fresh nodes =====\n"
# Dave discovers carol through zen exchange (seed-us relays carol's token
# during sync). Following carol by pubkey enters her into dave's follow
# graph; the follow auto-triggers a sync that resolves her token through a
# known token and dials her. Carol posts after that resolving round, so an
# explicit dave sync pulls her new post.
dnq dave follow "$CAROL_ID"
dnq carol post "carol's first post"
dnq dave sync
sleep 10
DAVE_FEED3=$(dn dave feed)
contains "$DAVE_FEED3" "carol's first post" && ok "P3.1 dave sees carol's post (followed)" || bad "P3.1 dave sees carol" "$DAVE_FEED3"

printf "\n===== Phase 4: Bidirectional follow =====\n"
dnq carol follow "$DAVE_ID"
dnq dave post "dave posts here"
sleep 10
CAROL_FEED3=$(dn carol feed)
contains "$CAROL_FEED3" "dave posts here" && ok "P4.1 carol sees dave's post (followed)" || bad "P4.1 carol sees dave" "$CAROL_FEED3"

printf "\n===== Phase 5: Eve discovers the network =====\n"
dnq eve post "eve checking in"
dnq eve follow "$CAROL_ID"
sleep 10
dnq carol follow "$EVE_ID"
sleep 10
CAROL_FEED4=$(dn carol feed)
contains "$CAROL_FEED4" "eve checking in" && ok "P5.1 carol sees eve's post" || bad "P5.1 carol sees eve" "$CAROL_FEED4"

printf "\n===== Phase 6: Persistent key stability =====\n"
SEED_EU_TOKEN_BEFORE=$(dn seed-eu whoami | grep '^address:' | awk '{print $2}')
docker compose stop seed-eu 2>/dev/null
sleep 2
docker compose start seed-eu 2>/dev/null
sleep 2
start_daemon seed-eu
sleep 5
SEED_EU_TOKEN_AFTER=$(dn seed-eu whoami | grep '^address:' | awk '{print $2}')
[[ "$SEED_EU_TOKEN_BEFORE" == "$SEED_EU_TOKEN_AFTER" ]] && ok "P6.1 seed-eu token stable across restart" || bad "P6.1 token stable" "before: $SEED_EU_TOKEN_BEFORE after: $SEED_EU_TOKEN_AFTER"

printf "\n===== Phase 7: Idempotency =====\n"
CAROL_COUNT_BEFORE=$(dn carol feed | wc -l | tr -d ' ')
dnq carol sync
sleep 5
CAROL_COUNT_AFTER=$(dn carol feed | wc -l | tr -d ' ')
[[ "$CAROL_COUNT_BEFORE" == "$CAROL_COUNT_AFTER" ]] && ok "P7.1 idempotent re-sync" || bad "P7.1 idempotent" "before: $CAROL_COUNT_BEFORE after: $CAROL_COUNT_AFTER"

printf "\n===== Phase 8: Late post propagation =====\n"
dnq carol post "carol late post"
sleep 10
DAVE_FEED4=$(dn dave feed)
contains "$DAVE_FEED4" "carol late post" && ok "P8.1 dave sees carol's late post" || bad "P8.1 dave sees late post" "$DAVE_FEED4"

printf "\n===== Phase 9: Follow graph =====\n"
# Followings are deterministic: each is a signed ProfileLog event the node
# authored, so the count is exact. Followers are observational (depend on
# sync propagation), so we assert the mutual relationships that were
# established via explicit bidirectional syncs are present.

expect_count() { # <node> <subcmd> <want>
  local got
  got=$(dn "$1" "$2" | grep -c '^driftnode:')
  [[ "$got" -eq "$3" ]] && ok "$1 $2 = $3" || bad "$1 $2 = $3" "got $got"
}
expect_has() { # <node> <subcmd> <identity>
  dn "$1" "$2" | grep -qF "$3" \
    && ok "$1 $2 has $3" \
    || bad "$1 $2 has $3" "missing"
}

expect_count seed-eu follows 1
expect_count seed-us follows 1
expect_count carol   follows 4
expect_count dave    follows 3
expect_count eve     follows 2

expect_has dave  followers "$CAROL_ID"
expect_has carol followers "$DAVE_ID"
expect_has carol followers "$EVE_ID"
expect_has eve   followers "$CAROL_ID"

printf "\n===== TOTAL: %d passed, %d failed =====\n" "$PASS" "$FAIL"
[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1
