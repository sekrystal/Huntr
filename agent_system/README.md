# Code Agent Layer

This folder contains a repo-local code-agent system for working on Opportunity Scout itself.

It is separate from the product's own signal, listing, and lead logic.

## Structure

- `prompts/`: role prompts for Planner, Builder, Debugger, QA, Refactor, and Docs
- `skills/`: repeatable maintenance and validation workflows
- `tasks/`: lightweight task scaffolding
- `reports/`: lightweight report scaffolding

## How To Use It

Recommended flow:

1. start with [`prompts/planner.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/prompts/planner.md)
2. implement with [`prompts/builder.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/prompts/builder.md)
3. run local verification with [`prompts/debugger.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/prompts/debugger.md)
4. validate acceptance criteria with [`prompts/qa.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/prompts/qa.md)
5. simplify with [`prompts/refactor.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/prompts/refactor.md)
6. update docs with [`prompts/docs.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/prompts/docs.md)

## Useful Starter Skills

- [`skills/reset_demo_db/SKILL.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/skills/reset_demo_db/SKILL.md)
- [`skills/run_local_stack/SKILL.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/skills/run_local_stack/SKILL.md)
- [`skills/validate_pdf_resume/SKILL.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/skills/validate_pdf_resume/SKILL.md)
- [`skills/audit_freshness/SKILL.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/skills/audit_freshness/SKILL.md)
- [`skills/validate_saved_applied_tracker/SKILL.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/skills/validate_saved_applied_tracker/SKILL.md)
- [`skills/demo_weak_signal_to_lead/SKILL.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/skills/demo_weak_signal_to_lead/SKILL.md)
- [`skills/ui_clutter_audit/SKILL.md`](/Users/samuelkrystal/Huntr/opportunity-scout/agent_system/skills/ui_clutter_audit/SKILL.md)

## Usage Pattern

For a new task:

1. create a task note from `tasks/TASK_TEMPLATE.md`
2. use the Planner prompt to define a bounded plan
3. use Builder to implement
4. use Debugger and relevant skills to run locally
5. use QA to validate acceptance criteria
6. write a brief completion note from `reports/REPORT_TEMPLATE.md`

## Keep It Honest

This layer should help agents do better work with less supervision.

It should not:

- invent fake infra
- hide missing validation
- justify clutter
- override the product direction defined in [`AGENTS.md`](/Users/samuelkrystal/Huntr/opportunity-scout/AGENTS.md)
