# Improvement Cycle Report - 2026-03-18

## Task

Use the internal code-agent layer to run a full improvement cycle on Opportunity Scout covering:

- stale listing behavior
- table-first UI simplification
- PDF resume parsing validation
- saved and applied tracker validation

## Planner Summary

Acceptance criteria:

- expired and stale leads remain hidden by default
- the workbench stays table-first and easier to scan
- PDF parsing still works and candidate-profile startup calls succeed
- saved and applied flows remain visible and usable
- local demo mode still resets, boots, and validates cleanly

## What Changed

- kept the main workbench table-first and removed more table noise from the visible shortlist
- added `saved_on` and `applied_on` columns to the `Saved` and `Applied` views
- kept explicit `Open` links in the live grid and detail view
- improved shortlist ordering to favor stronger, fresher, and more recent leads
- replaced the visible “weird title” seed example with a more credible `Strategic Operations Lead`
- updated README run and demo notes to match the current UI and port usage

## Files Touched

- `ui/app.py`
- `services/sync.py`
- `connectors/greenhouse.py`
- `README.md`

## Validation

- tests: `./.venv/bin/pytest` -> passed
- reset: `./.venv/bin/python scripts/reset_demo.py` -> passed
- API: `/health`, `/candidate-profile`, `/leads`, `/leads?only_saved=true`, `/leads?only_applied=true` -> passed
- UI: Streamlit responded on port `8500`
- freshness audit: default shortlist excluded `ArchiveCo`, `Growth Intern`, and `Rocket Propulsion Engineer`; expanded query showed they still exist as hidden seeded validation records

## Remaining Notes

- default API results still include the signal-only lead; the Streamlit workbench hides that by default through its type filter
- Streamlit does not offer true spreadsheet-style inline per-column filter widgets, so the current implementation uses a compact filter row above the table

