#!/usr/bin/env bash
#
# gen-proto.sh regenerates the gRPC + protobuf Go bindings from the .proto
# using the vendored, pinned protoc binary. No external install is required.
#
# Run via: go generate ./...  (or ./scripts/gen-proto.sh directly)
set -euo pipefail

# Resolve the repo root from the script location, not the working directory.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
proto_root="$repo_root/internal/proto"
proto_file="$proto_root/driftnode.proto"
out_dir="$proto_root/driftnodepb"

# Select the vendored protoc for the current host.
protoc_bin="$proto_root/bin/protoc"
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) protoc_bin="$proto_root/bin/darwin/protoc" ;;
  Darwin-x86_64) protoc_bin="$proto_root/bin/darwin/protoc" ;;
  Darwin-arm) protoc_bin="$proto_root/bin/darwin/protoc" ;;
  Linux-x86_64) protoc_bin="$proto_root/bin/linux-amd64/protoc" ;;
  Linux-aarch64) protoc_bin="$proto_root/bin/linux-arm64/protoc" ;;
  *) echo "gen-proto: no vendored protoc for $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

# The include dir ships the standard .proto files (google/protobuf/*.proto)
# that protoc needs for well-known types. Put it adjacent to the binary.
include_dir="$(dirname "$protoc_bin")/include"

# Build the two Go plugins into a throwaway bin dir pinned by the go.mod
# toolchain versions.
plugin_bin="$proto_root/.bin"
rm -rf "$plugin_bin"
mkdir -p "$plugin_bin"
GOBIN="$plugin_bin" go install google.golang.org/protobuf/cmd/protoc-gen-go
GOBIN="$plugin_bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc

mkdir -p "$out_dir"

# protoc resolves plugins via PATH; prepend the plugin dir so the vendored
# versions win over anything on the host.
PATH="$plugin_bin:$PATH" "$protoc_bin" \
  "--proto_path=$proto_root" \
  "--proto_path=$include_dir" \
  "--go_out=$out_dir" \
  "--go_opt=paths=source_relative" \
  "--go-grpc_out=$out_dir" \
  "--go-grpc_opt=paths=source_relative" \
  "$proto_file"

rm -rf "$plugin_bin"
echo "gen-proto: generated $out_dir"
