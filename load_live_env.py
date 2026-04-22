#!/usr/bin/env python3
"""
Read local Polymarket live-trading secrets from vars.json and print shell export lines.

The output matches what bot_v2.get_live_runtime_config() expects (see README).

vars.json field mapping (any one column per value is accepted):

| Canonical env         | JSON keys (first match wins)                    |
| POLY_PRIVATE_KEY      | private_key, POLY_PRIVATE_KEY                   |
| POLY_FUNDER           | proxy_key, poly_funder, funder, POLY_FUNDER       |
| POLY_API_KEY          | POLY_API_KEY, API_KEY, api_key                 |
| POLY_SECRET           | POLY_SECRET, SECRET, api_secret                |
| POLY_PASSPHRASE       | POLY_PASSPHRASE, Passphrase, passphrase         |
| POLY_SIGNATURE_TYPE   | signature_type, POLY_SIGNATURE_TYPE            |
| VC_KEY                | vc_key, VC_KEY (Visual Crossing API key)        |

signature_type: number, or "proxy" (1) / "eoa" (0) (case-insensitive).
"""

from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path

DEFAULT_PATH = Path(__file__).resolve().parent / "vars.json"

_CANON = [
    "POLY_PRIVATE_KEY",
    "POLY_FUNDER",
    "POLY_API_KEY",
    "POLY_SECRET",
    "POLY_PASSPHRASE",
    "POLY_SIGNATURE_TYPE",
    "VC_KEY",
]

_ALIASES = {
    "POLY_PRIVATE_KEY": ("POLY_PRIVATE_KEY", "private_key"),
    "POLY_FUNDER": ("POLY_FUNDER", "proxy_key", "poly_funder", "funder"),
    "POLY_API_KEY": ("POLY_API_KEY", "API_KEY", "api_key"),
    "POLY_SECRET": ("POLY_SECRET", "SECRET", "api_secret"),
    "POLY_PASSPHRASE": ("POLY_PASSPHRASE", "Passphrase", "passphrase", "api_passphrase"),
    "POLY_SIGNATURE_TYPE": ("POLY_SIGNATURE_TYPE", "signature_type"),
    "VC_KEY": ("VC_KEY", "vc_key"),
}


def _get_first(data: dict, keys: tuple[str, ...]) -> str:
    for k in keys:
        if k not in data:
            continue
        v = data[k]
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _parse_signature_type(raw: str) -> str:
    t = raw.strip()
    if not t:
        return "1"  # match README / bot default for proxy
    low = t.lower()
    if low == "proxy":
        return "1"
    if low in ("eoa", "0"):
        return "0"
    if low in ("1", "2", "3"):
        return t
    try:
        n = int(t, 10)
        return str(n)
    except ValueError as e:
        raise SystemExit(
            f"Invalid signature_type: {raw!r} (expected 0-3, or proxy/eoa)"
        ) from e


def load_resolved(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise SystemExit(f"vars file not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit("vars.json must be a JSON object")

    out: dict[str, str] = {}
    for canon, alts in _ALIASES.items():
        v = _get_first(data, alts)
        if not v:
            continue
        if canon == "POLY_SIGNATURE_TYPE":
            v = _parse_signature_type(v)
        out[canon] = v
    return out


def _check(resolved: dict[str, str]) -> list[str]:
    problems: list[str] = []
    if not resolved.get("POLY_PRIVATE_KEY"):
        problems.append("missing POLY_PRIVATE_KEY (set private_key in vars.json or env)")
    st = resolved.get("POLY_SIGNATURE_TYPE", "1")
    if st == "1" and not resolved.get("POLY_FUNDER"):
        problems.append("proxy mode (signature_type=1) requires POLY_FUNDER (proxy_key in vars.json)")

    trio = [
        bool(resolved.get("POLY_API_KEY")),
        bool(resolved.get("POLY_SECRET")),
        bool(resolved.get("POLY_PASSPHRASE")),
    ]
    if any(trio) and not all(trio):
        problems.append("POLY_API_KEY, POLY_SECRET, and POLY_PASSPHRASE must be all set or all empty (derive at runtime)")

    return problems


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "path",
        nargs="?",
        type=Path,
        default=DEFAULT_PATH,
        help=f"path to vars.json (default: {DEFAULT_PATH})",
    )
    ap.add_argument(
        "--check",
        action="store_true",
        help="validate required values and exit 0/1; print no exports",
    )
    ap.add_argument(
        "--comment",
        action="store_true",
        help="prefix each line with a comment (for copy-paste into a script file)",
    )
    args = ap.parse_args()

    resolved = load_resolved(args.path)
    problems = _check(resolved)
    if args.check:
        if problems:
            for p in problems:
                print(p, file=sys.stderr)
            sys.exit(1)
        print("ok")
        sys.exit(0)

    for p in problems:
        print(f"# warning: {p}", file=sys.stderr)

    for name in _CANON:
        if name not in resolved:
            continue
        val = shlex.quote(resolved[name])
        if args.comment:
            print(f"# export {name}=...")
        print(f"export {name}={val}")


if __name__ == "__main__":
    main()
