# =============================================================================
# Transform schedule (slice 8, ADR-0002)
#
#   daily at 00:30 UTC, indefinitely
#
# The hourly MEASUREMENT WINDOW (ticket #5) ran its 24 runs from 2026-11-11 06:00 to
# 2026-11-12 05:00 UTC and was then removed (ticket #8). Its results live in METRICS
# M-064, not in this file. To measure an hourly cadence again, restore it from git
# history (commit 2c2fd46) with a fresh start date.
#
# WHY NOT HOURLY FOREVER: cost. Hourly would blow the $10 budget roughly tenfold;
# daily fits. Measured figures: ADR-0002 (update of 2026-11-12) and METRICS M-064.
#
# WHY 00:30 FOR THE DAILY RUN: Bronze is partitioned by ingest date, so day D's last
# minutes arrive just after midnight. A run at 00:30 rewrites D after it is complete,
# the same reasoning as the backfill's as_of (scripts/backfill_silver.sh). The 26-hour
# lookback (ticket #4) covers the whole of D from there.
#
# NO start_date: a cron schedule simply fires at its next 00:30 UTC. Earlier the start
# date came from the window variable, which tied this permanent schedule to a one-off
# setting and made every later plan prompt for it (ticket #8).
#
# The schedule starts the state machine with input {}, so every run gets the default
# 26-hour lookback.
# =============================================================================

resource "aws_iam_role" "transform_scheduler" {
  name               = "${var.project_name}-transform-scheduler"
  description        = "EventBridge Scheduler: start the transform chain state machine. Nothing else."
  assume_role_policy = data.aws_iam_policy_document.scheduler_trust.json
}

resource "aws_iam_role_policy" "transform_scheduler" {
  name = "${var.project_name}-transform-scheduler-policy"
  role = aws_iam_role.transform_scheduler.id

  policy = templatefile("${path.module}/../iam_policies/transform_scheduler.json", {
    state_machine_arn = aws_sfn_state_machine.transform_chain.arn
  })
}

resource "aws_scheduler_schedule" "transform_daily" {
  name        = "${var.project_name}-transform-daily"
  description = "Daily transform run at 00:30 UTC (ADR-0002)."

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(30 0 * * ? *)"
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_sfn_state_machine.transform_chain.arn
    role_arn = aws_iam_role.transform_scheduler.arn
    input    = jsonencode({})

    # Retries here only cover failing to START an execution. Failures inside a run
    # are the state machine's job (ticket #3). A start more than 5 minutes late is
    # dropped; tomorrow's run catches up thanks to the 26-hour lookback. A dropped
    # start emails through the alarm in scheduler_alarm.tf (ticket #10).
    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 300
    }
  }

  depends_on = [aws_iam_role_policy.transform_scheduler]
}
