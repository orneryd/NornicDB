#!/bin/bash
# Generate Go code from Qdrant proto definitions
#
# Prerequisites:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#
# Usage:
#   ./generate.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Ensure protoc plugins are in PATH
export PATH="$PATH:$(go env GOPATH)/bin"

echo "Generating Go code from qdrant.proto..."

cd "$ROOT_DIR"

protoc \
    --go_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_out=. \
    --go-grpc_opt=paths=source_relative \
    pkg/qdrantgrpc/proto/qdrant.proto

# Move generated files to gen directory
mv pkg/qdrantgrpc/proto/*.pb.go pkg/qdrantgrpc/gen/

echo "Done! Generated files in pkg/qdrantgrpc/gen/"

