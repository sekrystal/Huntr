# Task

## Summary

Reduce activity log noise and improve investigation readability without breaking existing behavior.

## Goal

Make unattended runs easier to inspect by cutting repeated no-op activity spam and making unresolved signal investigations easier to understand in the UI.

## Acceptance Criteria

- repeated no-op activity entries do not flood the activity feed during repeated scheduler cycles
- investigations are easier to scan and show the next expected step
- existing tests still pass
- reset, API boot, and Streamlit boot still work

## Constraints

- no decorative UX changes
- preserve existing lead, tracking, and learning behavior
- prefer guardrails and readability over new features

## Suggested Agent Flow

1. Planner: isolate unattended-operation noise and investigation readability issues
2. Builder: add activity dedupe and cleaner investigation presentation
3. Debugger: run tests, soak test, and local stack
4. QA: verify investigations and activity remain legible after repeated cycles
5. Refactor: trim any noisy or redundant output
6. Docs: update README to describe scheduler guardrails and soak testing

## Notes

- this task was executed as part of the autonomy hardening pass
