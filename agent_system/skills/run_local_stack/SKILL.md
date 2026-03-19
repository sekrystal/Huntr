# run_local_stack

Use this skill when you need to boot Opportunity Scout locally for debugging or validation.

## Goal

Run the repo in local demo mode with FastAPI and Streamlit.

## Steps

1. activate the virtualenv
2. run `python scripts/reset_demo.py` when a clean state is helpful
3. start FastAPI on `127.0.0.1:8000`
4. start Streamlit on the chosen port, usually `8500`
5. verify:
   - `GET /health`
   - `GET /candidate-profile`
   - Streamlit root responds

## Notes

- avoid assuming `8501` is free
- if ports are in use, inspect or restart the existing processes instead of stacking duplicates

