---
name: feature-workflow
description: End-to-end delivery pipeline for any open-minions feature or bugfix — backlog item, branch, plan, implement, test, verify, review, security audit, docs, PR, CI, merge. Use whenever starting work on a backlog task, a milestone, or any code change, and when the user says "build", "implement", "fix", "next milestone" or "continue".
---

# Feature workflow (orchestrator playbook)

The main session is the **orchestrator**. It delegates to subagents (`.claude/agents/`) and keeps only
their conclusions in context. Model allocation is defined in each agent's frontmatter:

| Step | Agent | Model | Why |
|---|---|---|---|
| Plan | `planner` | Opus | Design quality, spec reasoning |
| Code | `implementer` | Sonnet | Fast, cheap, strong at code |
| Tests | `test-engineer` | Sonnet | Volume of test code |
| Verify | `verifier` | Sonnet | Runs commands, collects evidence |
| Review | `reviewer` | Opus | Finds subtle concurrency/spec bugs |
| Security | `security-auditor` | Opus | Threat reasoning |
| Docs | `docs-keeper` | Haiku | Mechanical sync |

## Steps

1. **Backlog item** (always first). Existing task, or
   `backlog task create "<title>" -d "..." --ac "..." -l <label>`; then
   `backlog task edit <id> -s "In Progress"`. Commit the new/changed task file on main
   (`chore(backlog): add TASK-<id>`) and push before branching.
2. **Branch** from up-to-date main: `git switch main && git pull --ff-only && git switch -c <type>/task-<id>-<slug>`
   (`feat|fix|chore|docs|test|refactor`). One task = one branch = one PR.
3. **Plan** → `planner` with the task id. Read its summary. If it proposes sub-tasks, create them and
   run this workflow per sub-task (each its own branch/PR), or keep them as steps if small.
4. **Implement** → `implementer` per step (sequential; steps touching disjoint packages may run in
   parallel). Each step ends with green unit tests and a commit.
5. **Tests** → `test-engineer` when the change touches SQL, the executor protocol, watchdog, or
   capacity governor, or when an AC has no test.
6. **Verify** → `verifier` (runs `verifieer-feature`). FAIL → back to step 4 with the evidence.
7. **Review + security** → launch `reviewer` and `security-auditor` **in parallel** (one message, two
   Agent calls). Security audit may be skipped only for docs-only or test-only changes.
   BLOCKER/MAJOR/CRITICAL/HIGH → `implementer` fixes → re-run verify + the agent that raised them.
   MINOR/NIT/LOW/MEDIUM: fix if cheap, else create a follow-up backlog task.
8. **Docs** → `docs-keeper`.
9. **PR**: `git push -u origin HEAD` and
   `gh pr create --title "<conventional title> (TASK-<id>)" --body "<summary, AC checklist, test evidence, review verdicts>"`.
   PR body ends with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
10. **CI**: `gh pr checks --watch`. Red → fix on the branch (back to 4), push, repeat.
11. **Merge** when CI is green and both verdicts are APPROVE/PASS:
    `gh pr merge --squash --delete-branch`, then `git switch main && git pull --ff-only`.
12. **Close**: `backlog task edit <id> -s Done`; commit the backlog change on main
    (`chore(backlog): close TASK-<id>`) and push.

## If project agents are not loaded
Agent types in `.claude/agents/` load at session start. If `subagent_type: "planner"` (etc.) is
rejected, use `subagent_type: "general-purpose"` with the agent's `model` (opus/sonnet/haiku) and a
prompt starting: "First read `.claude/agents/<name>.md` and act exactly as that agent."

## Rules
- Never commit directly to main except backlog bookkeeping (steps 1 and 12).
- Never merge with a failing check, an open BLOCKER/CRITICAL, or unticked acceptance criteria
  (unless marked NOT VERIFIABLE HERE with a reason in the PR body).
- Keep the orchestrator context lean: ask agents for short reports; do not paste full diffs back.
- If an agent is stuck twice on the same problem, the orchestrator investigates directly.
