# =============================================================================
# Scheduler start-failure alarm (ticket #10)
#
# If EventBridge Scheduler cannot START a run (throttled, a role or policy broken,
# IAM not yet propagated), it retries per the schedule's retry policy and then drops
# the event. No execution exists, so neither the chain's alerts nor the run-failed
# rule (ticket #9) can see it. A daily run that silently never starts leaves Gold
# stale for days. This alarm emails when that happens.
#
# WHY InvocationDroppedCount, NOT TargetErrorCount: TargetErrorCount counts every
# failed attempt, including ones the next retry fixes. Dropped means the scheduler
# gave up: a run really did not start.
# A start is dropped when its retry policy is exhausted, by attempts OR by event age
# (maximum_event_age_in_seconds). IF A DEAD-LETTER QUEUE IS EVER ADDED to a schedule,
# exhausted events go there instead (InvocationsSentToDeadLetterCount): revisit this
# alarm then. AWS delivers Scheduler metrics on a best-effort basis.
#
# SCOPE: Scheduler publishes these metrics per SCHEDULE GROUP only. Both schedules
# (daily transform and 5-minute ingestion) are in the `default` group, so this one
# alarm covers both. A dropped ingestion invocation loses no data on its own (each
# invocation fetches 15 bars, so the next two overlap it), but it means the scheduler
# gave up on invoking the Lambda (attempts or event age ran out), which is worth
# knowing. Baseline: 23,140 invocations from 2026-08-22 to 2026-11-10, none dropped
# (M-068).
#
# WINDOW: 12 periods of 5 minutes, alarm on any 1. A drop on time emails within
# ~5 minutes; a data point AWS delivers late still lands inside the last hour, instead
# of in a single 5-minute window that has already been judged "no data = OK".
#
# Emails on ALARM only, never on recovery (ADR-0001: email only when something is
# wrong). Cost: the account had no CloudWatch alarms before this one (M-068), and
# AWS's free tier includes 10 alarm metrics.
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "scheduler_invocation_dropped" {
  alarm_name        = "${var.project_name}-scheduler-invocation-dropped"
  alarm_description = "A scheduled start (daily transform run or 5-min ingestion invocation) was dropped after all retries, so it never started. See ticket #10 and the schedule's target in EventBridge Scheduler."

  namespace   = "AWS/Scheduler"
  metric_name = "InvocationDroppedCount"
  # Taken from the schedules, not typed: if they ever move to another group, the
  # alarm follows (and the precondition below stops a split across two groups).
  dimensions = {
    ScheduleGroup = aws_scheduler_schedule.transform_daily.group_name
  }

  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 12
  datapoints_to_alarm = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0

  # The metric only has data points when something is dropped. No data = nothing
  # dropped = OK, so the alarm doesn't sit in INSUFFICIENT_DATA or flap.
  treat_missing_data = "notBreaching"

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]

  lifecycle {
    precondition {
      condition     = aws_scheduler_schedule.ingestion.group_name == aws_scheduler_schedule.transform_daily.group_name
      error_message = "The ingestion and daily transform schedules are in different schedule groups; one alarm per group is needed (ticket #10)."
    }
  }
}
