# =============================================================================
# Transform schedules (slice 8, ticket #5, ADR-0002)
#
#   measurement window:  hourly at :00, exactly var.measurement_window_runs times,
#                        then it stops by its own end date
#   afterwards:          daily at 00:30 UTC, indefinitely
#
# WHY NOT HOURLY FOREVER: one run costs ~$0.131 (M-063), so hourly would be ~$94/month
# against a $10 budget. The hourly design is MEASURED once, over a bounded window, and
# the pipeline runs daily after that (~$3.93/month).
#
# WHY END DATES, NOT A REMINDER: the window switches itself off. If nobody remembers
# to apply again, nothing keeps billing.
#
# WHY 00:30 FOR THE DAILY RUN: Bronze is partitioned by ingest date, so day D's last
# minutes arrive just after midnight. A run at 00:30 rewrites D after it is complete,
# the same reasoning as the backfill's as_of (scripts/backfill_silver.sh). The 26-hour
# lookback (ticket #4) covers the whole of D from there.
#
# Both schedules start the state machine with input {}, so every scheduled run gets
# the default 26-hour lookback.
# =============================================================================

locals {
  # A schedule may begin invoking AFTER its start date. Opening each schedule 5 minutes
  # before its first intended invocation and closing it 30 minutes after its last
  # makes the count exact without depending on whether the boundaries are inclusive.
  window_open  = timeadd(var.measurement_window_start, "-5m")
  window_close = timeadd(var.measurement_window_start, format("%dm", (var.measurement_window_runs - 1) * 60 + 30))
  daily_start  = timeadd(var.measurement_window_start, format("%dh", var.measurement_window_runs))
}

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

resource "aws_scheduler_schedule" "transform_window_hourly" {
  name        = "${var.project_name}-transform-window-hourly"
  description = "Measurement window (ADR-0002): ${var.measurement_window_runs} hourly runs from ${var.measurement_window_start}, then stops."

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 * * * ? *)"
  schedule_expression_timezone = "UTC"
  start_date                   = local.window_open
  end_date                     = local.window_close

  target {
    arn      = aws_sfn_state_machine.transform_chain.arn
    role_arn = aws_iam_role.transform_scheduler.arn
    input    = jsonencode({})

    # Retries here only cover failing to START an execution. Failures inside a run
    # are the state machine's job (ticket #3). A start that is more than 5 minutes
    # late is dropped rather than queued: the next hour's run catches up anyway,
    # thanks to the 26-hour lookback.
    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 300
    }
  }

  depends_on = [aws_iam_role_policy.transform_scheduler]
}

resource "aws_scheduler_schedule" "transform_daily" {
  name        = "${var.project_name}-transform-daily"
  description = "Daily transform run at 00:30 UTC, from the end of the measurement window onward (ADR-0002)."

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(30 0 * * ? *)"
  schedule_expression_timezone = "UTC"
  start_date                   = local.daily_start

  target {
    arn      = aws_sfn_state_machine.transform_chain.arn
    role_arn = aws_iam_role.transform_scheduler.arn
    input    = jsonencode({})

    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 300
    }
  }

  depends_on = [aws_iam_role_policy.transform_scheduler]
}
