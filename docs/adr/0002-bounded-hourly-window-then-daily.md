# One bounded hourly window, then a daily run

The README designed the transform chain to run hourly, indefinitely. We run it hourly for only one 24-run measurement window, then once a day permanently, both driven by EventBridge Scheduler start and end dates. The reason is cost at the current scale. Each run measured Silver at 308 s (M-057) and Gold at 133 s (M-058), about $0.108 per run, so running hourly forever would cost about $78 a month against a $10 budget. M-020's "$33/month hourly" was derived from 1-pair, 94-second runs in August and no longer holds at 17 pairs.

## Consequences

- **Lookback:** every run uses a 26-hour Silver lookback instead of 3, so a daily run covers its whole day and any run catches up after a failed predecessor. Each run therefore rewrites two Silver days instead of usually one; the effect on runtime is to be measured, not assumed. A manual run can override the lookback (`lookback_hours`) to catch up a longer outage. Multi-day gaps still go through `scripts/backfill_silver.sh`, to stay inside the 25-minute Glue timeout.
- **Freshness of the output:** Gold is up to about a day old outside the window. That is acceptable for a review filter.
- **Measurement:** the hourly design's claims (run duration, data delay, gate pass rate, cost per run) are measured once, with n = 24, rather than demonstrated continuously.
- **Gold stays a full rebuild:** it rescores all of Silver on every run (133 s over 79 days). Making it incremental is deferred until per-run measurements show the rebuild becoming too slow.

## Update, 2026-11-12: the window ran and was removed

The window ran 24/24 runs from 2026-11-11 06:00 to 2026-11-12 05:00 UTC (M-064). The measured cost is p50 $0.1493 per run, higher than the $0.108 above (M-060): the 26-hour lookback roughly doubled Silver (M-063). Hourly forever would now cost about $107 a month and daily about $4.47 (M-064, DERIVED), so the decision stands.

The hourly schedule and its window variable were then deleted (ticket #8). The daily schedule has no start date of its own and fires at the next 00:30 UTC. To measure an hourly cadence again, restore the window schedule from git history with a new start date.
