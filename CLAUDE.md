# market_anomaly_pipeline

A portfolio data-engineering project. It ingests Binance 1-minute bars for 17 pairs into S3 (Bronze), cleans them with Glue (Silver), and flags z-score anomalies (Gold). AWS account 290586778476, us-east-1, Terraform with local state. The output is a filter for a person to review, not a trading signal. Never describe it as one.

## Working rules

- **The owner is learning AWS and data engineering** and needs to defend every design choice in interviews. Explain each decision and the alternatives you rejected. Go deep on the Spark windows, the z-scores, the DQ gate and IAM; keep plumbing brief. For walkthroughs, go one step at a time.
- **Write code and tests locally first** (`./scripts/run_tests.sh`). The owner runs `terraform plan/apply`. Invoking a Lambda or running a Glue job also needs the owner's go-ahead in that session.
- **Measure, don't estimate.** Every number goes into `METRICS.md` as an `M-0xx` entry labelled MEASURED, DERIVED, EXTRAPOLATED or ESTIMATED. Never round up. Log the prediction before measuring, and record misses.
- **`MEMORY.md` is append-only.** It is the chronological project story: add each event, decision and bug at the end. Never rewrite past entries; add a dated correction instead.
- **Keep `CLAUDE.md` current.** Edit rules here when they change. History belongs in `MEMORY.md`.
- **Resume bullets** follow Action + Task + Result: a strong verb and a measured outcome.

## Agent skills

### Issue tracker

Issues live in GitHub Issues for M1nhbui/market_anomaly_pipeline, managed with the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

The five default labels: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `GLOSSARY.md` and `docs/adr/` at the repo root, created only when needed. See `docs/agents/domain.md`.
