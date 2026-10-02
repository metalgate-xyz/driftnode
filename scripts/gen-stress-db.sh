#!/usr/bin/env bash
# gen-stress-db.sh - Generate a large driftnode store for TUI stress testing.
#
# Hardcoded huge numbers, no knobs. Produces a feed with ~1M entries across
# 10k followed zens using power-law distributions matched to real social
# network sizing. The local identity's outbound likes and replies follow a
# Zipf distribution so a few authors get most of the engagement and the long
# tail gets zero or one. Run from the repo root.
#
#   bash scripts/gen-stress-db.sh                 write data/stress.db
#   bash scripts/gen-stress-db.sh /tmp/big.db     write to a custom path
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

OUT="${1:-data/stress.db}"

go run scripts/gentestdb.go \
  -db "$OUT" \
  -overwrite \
  -zens 10000 \
  -posts 100 \
  -followers 0.3 \
  -likes 0.2 \
  -pins 0.1 \
  -own-like-max 50 \
  -own-reply-max 15 \
  -own-posts 50
