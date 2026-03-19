# Report

## Task

Reduce activity log noise and improve investigation readability without breaking existing behavior.

## What Changed

- added activity dedupe for repeated identical no-op entries in the recent window
- added autonomy health and latest run digest surfaces
- made investigations sort open work first and show the next expected step
- added soak-test controls and an accelerated scheduler soak runner

## Files Touched

- `services/activity.py`
- `services/autonomy.py`
- `services/scheduler.py`
- `ui/app.py`
- `api/routes/agents.py`
- `core/config.py`
- `.env.example`
- `README.md`

## Validation

- tests: `19 passed`
- local stack: FastAPI and Streamlit booted after the changes
- manual checks:
  - accelerated scheduler soak test ran 6 cycles
  - no duplicate lead keys were created
  - no stale rows became visible by default
  - investigations and learning remained populated

## Risks / TODOs

- activity dedupe reduces noise but does not fully summarize long histories yet
- scheduler health currently reflects config state, not a distributed worker fleet
- unattended operation is suitable for bounded local demos, not yet set-and-forget production use
