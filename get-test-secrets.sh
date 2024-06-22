#!/usr/bin/env bash
set -eufo pipefail
cd "$(dirname "$0")"
mkdir -p secrets

get_secret() {
    yc lockbox payload get "$1" --format json | jq '.entries | map({(.key): (.text_value)}) | add'
}

get_secret e6qe5ei28nq0ctucitlm >./secrets/mailer.json
