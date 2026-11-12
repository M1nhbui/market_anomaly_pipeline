# =============================================================================
# Run-failed alert (ticket #9, ADR-0001 update of 2026-11-12)
#
# The chain emails on its own when the gate blocks or a step crashes. Three kinds of
# ending never reach those email states:
#   - States.Runtime errors (e.g. a ResultSelector path that is missing). AWS:
#     "A retry or catch on States.ALL won't catch States.Runtime errors."
#   - TIMED_OUT and ABORTED executions
#   - any error name nobody has thought of yet
#
# This rule watches the chain's "Execution Status Change" events on the default event
# bus and emails on those. It SKIPS failures the chain already handled: GateBlocked and
# StepFailed (the chain emailed) and InvalidLookback (a rejected manual input, which
# by design sends no email). Without the skip, those runs would send two emails.
# tests/test_run_failed_alert.py keeps the skip-list equal to the chain's Fail errors.
#
# Cost: EventBridge rules on AWS service events are free; SNS email is within the free
# tier at this volume.
# =============================================================================

resource "aws_cloudwatch_event_rule" "run_failed_alert" {
  name        = "${var.project_name}-transform-run-failed"
  description = "Email on transform runs that end FAILED/TIMED_OUT/ABORTED without the chain's own alert (ticket #9)."

  event_pattern = templatefile("${path.module}/../state_machine/run_failed_alert.pattern.json", {
    state_machine_arn = aws_sfn_state_machine.transform_chain.arn
  })
}

resource "aws_cloudwatch_event_target" "run_failed_alert" {
  rule      = aws_cloudwatch_event_rule.run_failed_alert.name
  target_id = "pipeline-alerts"
  arn       = aws_sns_topic.pipeline_alerts.arn

  # EventBridge cannot set the email subject (SNS uses its default), so the first line
  # of the body says what this is.
  #
  # No `cause`: EventBridge "doesn't escape values extracted by Input Path", and a Glue
  # cause is a JSON string full of quotes that could break the message. The console
  # link shows the full cause. Each line is its own quoted string, the documented way
  # to send multi-line text.
  input_transformer {
    input_paths = {
      name      = "$.detail.name"
      status    = "$.detail.status"
      error     = "$.detail.error"
      execution = "$.detail.executionArn"
    }
    input_template = <<-EOT
      "[crypto-anomaly] Transform run <status> without the chain's own alert (runtime error, timeout or abort; ticket #9)."
      "Execution: <name>"
      "Error: <error>"
      "Console: https://${var.aws_region}.console.aws.amazon.com/states/home?region=${var.aws_region}#/v2/executions/details/<execution>"
    EOT
  }
}

# The topic's access policy, which grants this rule's publish, lives beside the topic in
# step_functions.tf.
