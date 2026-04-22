#!/usr/bin/env python3
"""Smoke test: derive Polymarket CLOB API credentials from a wallet key."""

import os
import sys

from eth_account import Account
from py_clob_client.client import ClobClient

# Polygon mainnet CLOB
CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137


def main() -> None:
    key = os.environ.get("POLY_PRIVATE_KEY", "").strip()
    if not key:
        print(
            "Set POLY_PRIVATE_KEY to your wallet private key (0x…), then run again.",
            file=sys.stderr,
        )
        sys.exit(1)

    account = Account.from_key(key)
    print("Wallet:", account.address)

    client = ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=key)
    api_creds = client.create_or_derive_api_creds()
    if api_creds is None:
        print("Failed to create or derive API credentials.", file=sys.stderr)
        sys.exit(1)

    print("API key:", api_creds.api_key)
    print("Secret:", api_creds.api_secret)
    print("Passphrase:", api_creds.api_passphrase)


if __name__ == "__main__":
    main()
