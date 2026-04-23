#!/usr/bin/env bash
set -euo pipefail

SERVICE="weatherbot-live"
ACCOUNT="${USER:-weatherbot}"

save_secret() {
  local key="$1"
  local value
  read -r -s -p "Enter ${key}: " value
  echo

  security add-generic-password \
    -U \
    -a "$ACCOUNT" \
    -s "${SERVICE}:${key}" \
    -w "$value" >/dev/null
}

echo "Saving Weatherbot live trading secrets to macOS Keychain..."
save_secret "PRIVATE_KEY"
save_secret "CHAIN_ID"
save_secret "SIGNATURE_TYPE"
save_secret "PROXY_KEY"
save_secret "POLY_API_KEY"
save_secret "POLY_SECRET"
save_secret "POLY_PASSPHRASE"

echo "Done. Secrets are stored in Keychain service '${SERVICE}'."
echo "Load them in your current shell with: source ./scripts/load_live_env.sh"
