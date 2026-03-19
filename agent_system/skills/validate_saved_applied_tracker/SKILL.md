# validate_saved_applied_tracker

Use this skill when changing tracker state, feedback handling, or the Saved/Applied views.

## Goal

Confirm save-for-later and applied tracking remain usable and visible in the workbench.

## Validation Checklist

1. reset demo data
2. verify seeded saved and applied rows exist
3. confirm `Saved` shows saved leads
4. confirm `Applied` shows applied leads and statuses
5. update one application status and verify it persists
6. ensure the primary leads table still reflects saved/applied state

## Pass Criteria

- saved and applied views are distinct
- applied rows can carry status and notes
- no saved/applied action silently fails

