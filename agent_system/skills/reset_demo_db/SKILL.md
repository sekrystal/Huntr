# reset_demo_db

Use this skill when you need a clean, current demo database before validating behavior.

## Goal

Reset Opportunity Scout to a known local demo state with no residual stale data.

## Steps

1. run `python scripts/reset_demo.py`
2. confirm the command succeeds
3. if the task concerns stale behavior, verify the expected seeded counts or sample records
4. restart running API and Streamlit processes if the task depends on live app behavior

## Expected Result

- SQLite demo DB wiped
- schema recreated
- fresh seeded records only
- no legacy `opportunities` table state

