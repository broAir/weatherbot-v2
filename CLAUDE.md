# Weatherbot — Agent Guidelines

## Project Snapshot
Weather-market trading bot for Polymarket. Core file: `bot_v2.py`. Config: `config.json`. State + markets: `data/`. Currently in **paper trading** phase — no real money moves.

## Context-Mode Rules (mandatory)
- Shell output >20 lines → `ctx_batch_execute` + `ctx_search`. Never pipe raw output into context.
- Never use `cat` on logs or large JSON — use `ctx_execute_file` for analysis.
- `Read` is for files you are about to `Edit`. Not for analysis.

## Karpathy Rules
- **Think first:** State assumptions before coding. Ask when unclear — 1 minute of asking saves 20 minutes of rework.
- **Simplicity:** Minimum code that solves the problem. No speculative features, no future-proofing.
- **Surgical:** Touch only lines the task requires. Don't clean up adjacent code. Match existing style.
- **Verify:** Run `python bot_v2.py status` and/or `python bot_v2.py report` after any meaningful change.

## Project Conventions
- **Config:** Read `config.json` before editing. Add new fields at end. Load with `_cfg.get(key, default)` in the CONFIG block (~line 42).
- **State:** Always call `save_state(state)` after mutating the state dict.
- **Markets:** Always call `save_market(mkt)` after mutating a market dict. Files live in `data/markets/`.
- **API:** Gamma endpoint `https://gamma-api.polymarket.com/markets/{market_id}`. Timeout `(3, 5)`. Fail gracefully — never crash on connection errors.

## Running the Bot — One Process Only
- **Never run multiple `bot_v2.py run` processes at once.** They share `data/state.json` and `data/markets/*.json` — concurrent writers corrupt state. They also multiply requests against the Polymarket Gamma + CLOB APIs, triggering rate-limits that look like socket hangs (especially on cities with open positions like Buenos Aires).
- Before `python bot_v2.py run`, verify nothing else is running: `ps aux | grep "bot_v2.py" | grep -v grep`. If anything appears, `pkill -9 -f "bot_v2.py"` and re-check.
- After `pkill`, ALWAYS re-check the count — `pkill` can leave zombies, especially when the bot was launched repeatedly via background tasks.
- If the bot appears stuck on a single city for >60s, the first thing to check is process count, NOT the city's API or a code timeout. Multiple processes is the usual culprit.

## Red Flags — Stop and Ask
- About to write >100 lines without running the code first
- Editing multiple files without reading them first
- Implementing something "for later" or "just in case"
- Adding `signal.alarm` or threading wrappers to "fix" a hang — diagnose process count first
