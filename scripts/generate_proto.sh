#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p app/pb
protoc --go_out=app/pb --go_opt=paths=source_relative --go-grpc_out=app/pb --go-grpc_opt=paths=source_relative proto/chat.proto
echo "Done."


