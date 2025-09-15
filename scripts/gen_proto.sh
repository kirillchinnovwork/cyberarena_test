#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
API_DIR="$ROOT_DIR/api"
PROTO_PATHS=(attachments auth news polygon users)

# Ensure tools are installed (best effort)
if ! command -v protoc >/dev/null 2>&1; then
  echo "protoc not found; please install protoc" >&2
  exit 1
fi

OUT_DIR="$API_DIR"

for svc in "${PROTO_PATHS[@]}"; do
  for vdir in "$API_DIR/$svc"/v1; do
    for f in "$vdir"/*.proto; do
      [ -f "$f" ] || continue
      echo "Generating for $f" >&2
      protoc \
        -I "$ROOT_DIR" \
        -I "$API_DIR" \
        -I "$GOMOD/cache/download" \
        -I "$GOPATH/pkg/mod" \
        --go_out "$ROOT_DIR" --go_opt paths=source_relative \
        --go-grpc_out "$ROOT_DIR" --go-grpc_opt paths=source_relative \
        --grpc-gateway_out "$ROOT_DIR" --grpc-gateway_opt paths=source_relative \
        --openapiv2_out "$vdir" \
        "$f"
    done
  done
done

echo "Proto generation complete"
