---
id: TASK-8
title: 'Agentic framework: project skills and subagents'
status: In Progress
assignee: []
created_date: '2026-10-15 14:08'
updated_date: '2026-10-15 14:11'
labels:
  - tooling
dependencies: []
priority: high
ordinal: 8000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Project skills (verifieer-feature, security-audit, code-quality, feature-workflow, db-migration) and subagents (planner, implementer, test-engineer, verifier, reviewer, security-auditor, docs-keeper) with per-role models: Opus for planning/review/security, Sonnet for coding/testing, Haiku for docs sync.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Skills in .claude/skills with SKILL.md and clear trigger descriptions
- [x] #2 Agents in .claude/agents with model, tools and responsibilities
- [x] #3 CLAUDE.md documents the pipeline
- [x] #4 Project settings allow routine dev commands
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Added 5 project skills (feature-workflow, verifieer-feature, code-quality, security-audit, db-migration), 7 subagents with per-role models (Opus: planner/reviewer/security-auditor; Sonnet: implementer/test-engineer/verifier; Haiku: docs-keeper), project permission allowlist, and pipeline docs in CLAUDE.md.
<!-- SECTION:FINAL_SUMMARY:END -->
