# Demo-Ready Cycle Report

## Task

Run a full code-agent cycle to confirm Opportunity Scout is demo-ready for employer presentation.

Acceptance criteria:

1. `reset_demo` fully wipes and reseeds the DB
2. expired and stale roles are hidden by default
3. PDF upload and parsing work
4. Leads view is table-first and easy to scan
5. Saved and Applied views work
6. weak signal to real lead demo path is visible
7. backend and Streamlit app both boot successfully
8. README accurately reflects the current product

## What Changed

- No product code changes were required in this cycle.
- Ran a full Planner -> Debugger -> QA validation loop against the current repo state.
- Confirmed the recent portability, lead filtering, and X-link handling changes are still working as intended.

## Files Touched

- [`agent_system/reports/DEMO_READY_CYCLE_2026-03-18.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/reports/DEMO_READY_CYCLE_2026-03-18.md)

## Validation

- tests:
  - `./.venv/bin/pytest` -> `10 passed`
- local stack:
  - `./.venv/bin/python scripts/reset_demo.py` -> passed
  - FastAPI boot on `http://127.0.0.1:8000` -> passed
  - Streamlit boot on `http://127.0.0.1:8500` -> passed
- manual checks:
  - default `/leads?freshness_window_days=14` returns combined and listing leads only
  - hidden audit endpoint shows expired and mismatched roles still exist but stay hidden by default
  - `Saved` API view returns seeded saved leads
  - `Applied` API view returns seeded applied lead
  - synthetic PDF extraction returns readable text through `pypdf`
  - Mercor combined lead includes the weak-signal-to-listing resolution story
  - README still matches reset, run, and product behavior

## Risks / TODOs

- Browser-level interaction was validated through live app boot and API-backed behavior, but not through automated click-driving.
- `streamlit.testing` in bare mode reported backend unavailability even while the live stack was healthy, so it is not a reliable substitute for real browser interaction in this repo today.
