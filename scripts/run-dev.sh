#!/usr/bin/env bash
set -eufo pipefail
cd "$(dirname "$0")/.."
set -x
go run . -logCmd 'go run .' -logCmdDir test/logCmd -reportTime '-1:-1:30' "$@"
