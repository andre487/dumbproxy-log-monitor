#!/usr/bin/env bash
set -eufo pipefail

cd "$(dirname "$0")"
set -x
go run . -log-cmd 'go run .' -log-cmd-dir testJournald -report-time '-1:-1:30' "$@"
