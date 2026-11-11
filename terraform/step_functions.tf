# =============================================================================
# Step Functions - the transform chain (slice 8, ADR-0001)
#
#   Defaults -> Config -> LookbackValid --(else)--> InvalidLookback (Fail)
#     -> Silver Glue job -> DQ gate Lambda -> Choice(quality_passed) -> Gold Glue job
#                                                  \-> AlertGateBlocked (SNS) -> GateBlocked (Fail)
#   Any step that crashes or times out -> AlertStepFailed (SNS) -> RunFailed (Fail)
#
# The definition lives in state_machine/transform_chain.asl.json, not inline here,
# so tests/test_state_machine.py can walk its paths offline and assert the promises
# (Gold only after a passed gate, Glue steps that wait for their job) on every test
# run, with no AWS calls.
#
# STANDARD, not EXPRESS. Express workflows cap at 5 minutes and do not support the
# run-and-wait (.sync) Glue integration; Silver alone takes ~5 minutes. Standard
# bills per state transition (~$0.025 per 1,000), so one run of ~5 transitions costs
# about $0.0001, against ~$0.108 of Glue (M-060).
#
# Started by the EventBridge Scheduler schedules in transform_schedules.tf (ticket #5),
# or by hand with an optional {"lookback_hours": N}.
#
# ALERTS (ticket #3): a blocked gate, or any step that crashes or times out, publishes
# ONE email to the pipeline alert topic and ends the run as failed. Successes and
# warnings never email (ADR-0001). Only transient AWS errors are retried.
# =============================================================================

data "aws_iam_policy_document" "step_functions_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }

    # Same confused-deputy guard as the scheduler role: only state machines owned by
    # this account may assume it.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "step_functions" {
  name               = "${var.project_name}-transform-chain"
  description        = "Step Functions transform chain: run the two Glue jobs and invoke the DQ gate. Nothing else."
  assume_role_policy = data.aws_iam_policy_document.step_functions_trust.json
}

resource "aws_iam_role_policy" "step_functions" {
  name = "${var.project_name}-transform-chain-policy"
  role = aws_iam_role.step_functions.id

  policy = templatefile("${path.module}/../iam_policies/step_functions.json", {
    silver_job_arn    = aws_glue_job.bronze_to_silver.arn
    gold_job_arn      = aws_glue_job.silver_to_gold.arn
    gate_function_arn = aws_lambda_function.dq_gate.arn
    alert_topic_arn   = aws_sns_topic.pipeline_alerts.arn
  })
}

resource "aws_sfn_state_machine" "transform_chain" {
  name     = "${var.project_name}-transform-chain"
  type     = "STANDARD"
  role_arn = aws_iam_role.step_functions.arn

  definition = templatefile("${path.module}/../state_machine/transform_chain.asl.json", {
    silver_job_name   = aws_glue_job.bronze_to_silver.name
    gold_job_name     = aws_glue_job.silver_to_gold.name
    gate_function_arn = aws_lambda_function.dq_gate.arn
    alert_topic_arn   = aws_sns_topic.pipeline_alerts.arn
  })

  # The role's policy must exist before the first execution, or the first run fails
  # with an AccessDenied that looks like a definition bug.
  depends_on = [aws_iam_role_policy.step_functions]
}

# -----------------------------------------------------------------------------
# Pipeline alerts
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project_name}-pipeline-alerts"
}

# EMAIL SUBSCRIPTIONS START "PendingConfirmation". AWS sends a confirmation email and
# delivers nothing until a human clicks the link in it - Terraform cannot click it.
# Until then every alert is silently dropped, so confirming is part of the deploy.
resource "aws_sns_topic_subscription" "pipeline_alerts_email" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
