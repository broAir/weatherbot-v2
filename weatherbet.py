#!/usr/bin/env python3
"""Compatibility entrypoint that forwards to bot_v2."""

import runpy


if __name__ == "__main__":
    runpy.run_module("bot_v2", run_name="__main__")
