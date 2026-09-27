//go:build tools

// Package tools pins build-time tool dependencies in go.mod so `go mod tidy`
// keeps them tracked. The `tools` build tag excludes this from the regular
// build.
package tools

import (
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)
