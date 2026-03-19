# ui_clutter_audit

Use this skill when the Streamlit workbench starts feeling noisy, decorative, or harder to scan.

## Goal

Keep the UI closer to a Clay-like workbench than a dashboard.

## Audit Questions

1. Is the main view table-first?
2. Are `Leads`, `Saved`, and `Applied` still the primary workflow views?
3. Can the user open the job link without hunting for it?
4. Are there redundant labels or columns?
5. Are explanations available without overwhelming the main grid?
6. Are filters doing real workflow work instead of exposing internal state?

## Preferred Outcomes

- fewer visible controls
- fewer redundant labels
- stronger hierarchy
- links and actions near the row they affect
- hidden complexity in expanders, not the main table
