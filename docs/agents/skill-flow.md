# Which Matt Pocock skill to use, and when

Most of these skills can only be started by the owner (they are marked `disable-model-invocation`). Claude can't run them, so at each moment below Claude stops and says: "type `/<skill> <args>` next", with one line on why. Claude does not do that stage by hand instead.

## Every slice, in order

| Step | Owner types | When |
|---|---|---|
| 1 | `/grill-with-docs` | Designing a slice. It also writes `GLOSSARY.md` terms and ADRs as decisions settle. |
| 2 | `/to-spec` | Once the design is agreed: publishes the spec issue. |
| 3 | `/to-tickets` | Right after the spec: splits it into tickets with native "blocked by" links. |
| 4 | `/implement #N` | Each unblocked ticket. It uses `/tdd` (tests first) at agreed seams. `/implement-spec` builds the whole spec on one branch instead. |
| 5 | `/code-review since <commit>, against #N` | Before every commit. |
| 6 | commit with `Closes #N` | The owner commits and pushes. |
| 7 | `/retro` | When a slice is finished: what went well, what to change next time. |

## When something specific happens

| Moment | Owner types |
|---|---|
| Unsure which skill fits | `/ask-matt` |
| A new bug, idea or request turns up mid-slice | `/triage` (labels it and turns it into a ticket) |
| A run fails, an alert email arrives, or a job gets slow | `/diagnosing-bugs` |
| A design question is easier to answer by trying it | `/prototype` |
| An AWS or library fact needs checking against official docs | `/research` |
| A one-off manual procedure (console clicks, secrets, CI setup) | `/wizard` |
| Planning work too big for one session (e.g. all of slice 9 plus resume) | `/wayfinder` |
| Claude's explanation didn't land | `/wait-what` |
| Learning a concept in depth (Spark windows, IAM, z-scores) | `/teach` |
| Stopping mid-task and resuming later in a fresh session | `/handoff` |
| Work moves onto branches and pull requests | `/pr` |
| Occasionally, to look for code worth restructuring | `/improve-codebase-architecture` |
| Once: block `git push` / `reset --hard` from Claude for good | `/git-guardrails-claude-code` |

Not used here: the TypeScript-only and writing skills (`setup-ts-deep-modules`, `migrate-to-shoehorn`, `scaffold-exercises`, `setup-pre-commit`, `writing-*`).
