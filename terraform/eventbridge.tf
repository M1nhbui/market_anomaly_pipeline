# =============================================================================
# EventBridge Scheduler - the ingestion cadence
#
# EventBridge Scheduler is AWS's managed cron. You declare "run this target on this
# rhythm" and AWS owns the timer. There is no always-on machine keeping time.
#
# Note this is EventBridge *Scheduler*, not the older EventBridge *Rules*. Rules
# still work and you will see them in most tutorials, but Scheduler is the current
# service: it supports one-time schedules, timezones with DST handling, a retry
# policy per target, and it does not require adding a resource-based permission
# policy to the Lambda itself. The tradeoff is that Scheduler needs its own IAM role
# to do the invoking, which Rules did not.
#
# ONLY ingestion is scheduled here. The Glue transform schedule is deliberately NOT
# created until slice 8, because Glue is the expensive component (~$42/month if left
# running hourly, per METRICS.md M-004) and there is no reason to pay it while we are
# still writing the job it would run.
# =============================================================================

data "aws_iam_policy_document" "scheduler_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    # Defence against the "confused deputy" problem.
    #
    # Without this, the trust policy says "any EventBridge Scheduler may assume this
    # role" - including a schedule in someone else's AWS account. That someone could
    # then invoke your Lambda. This condition restricts it to schedules owned by
    # THIS account. It is a small thing that turns up in real security reviews.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.project_name}-scheduler"
  description        = "EventBridge Scheduler: invoke the ingestion Lambda. Nothing else."
  assume_role_policy = data.aws_iam_policy_document.scheduler_trust.json
}

resource "aws_iam_role_policy" "scheduler" {
  name = "${var.project_name}-scheduler-policy"
  role = aws_iam_role.scheduler.id

  policy = templatefile("${path.module}/../iam_policies/eventbridge_scheduler.json", {
    ingestion_function_arn = aws_lambda_function.ingestion.arn
  })
}

resource "aws_scheduler_schedule" "ingestion" {
  name        = "${var.project_name}-ingestion-5min"
  description = "Fetch Binance klines into Bronze every 5 minutes."

  # OFF means "fire at exactly this time." The alternative is a flexible window,
  # where AWS may run the job anywhere inside a window you specify in order to
  # smooth its own load. Flexible is fine for genuinely non-urgent jobs and would be
  # fine here too, but exact timing makes the ingestion cadence easy to reason about
  # when we measure end-to-end latency later.
  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "rate(5 minutes)"
  schedule_expression_timezone = "UTC"

  # One flag to stop all ingestion without destroying anything. Set
  # ingestion_schedule_enabled = false in terraform.tfvars and apply.
  state = var.ingestion_schedule_enabled ? "ENABLED" : "DISABLED"

  target {
    arn      = aws_lambda_function.ingestion.arn
    role_arn = aws_iam_role.scheduler.arn

    # If an invocation fails outright (throttling, a Lambda service error),
    # Scheduler retries. Two attempts is plenty: our data has a 15-bar overlap, so
    # the NEXT run backfills anything this one missed anyway. Retries here are for
    # transient infrastructure faults, not for data completeness - the overlap is
    # what guarantees that.
    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 300
    }
  }
}
