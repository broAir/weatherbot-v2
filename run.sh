#!/bin/bash
set -e
cd "$(dirname "$0")"
eval "$(python3 load_live_env.py)"
exec .venv/bin/python bot_v2.py "$@"
