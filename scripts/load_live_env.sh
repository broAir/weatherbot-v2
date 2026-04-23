#!/usr/bin/env bash
set -euo pipefail

SERVICE="weatherbot-live"
ACCOUNT="${USER:-weatherbot}"

load_secret() {
  local key="$1"
  security find-generic-password \
    -a "$ACCOUNT" \
    -s "${SERVICE}:${key}" \
    -w
}

export PRIVATE_KEY="$(load_secret PRIVATE_KEY)"
export CHAIN_ID="$(load_secret CHAIN_ID)"
export SIGNATURE_TYPE="$(load_secret SIGNATURE_TYPE)"
export PROXY_KEY="$(load_secret PROXY_KEY)"
export POLY_API_KEY="$(load_secret POLY_API_KEY)"
export POLY_SECRET="$(load_secret POLY_SECRET)"
export POLY_PASSPHRASE="$(load_secret POLY_PASSPHRASE)"

echo "Weatherbot live env vars loaded into current shell."
