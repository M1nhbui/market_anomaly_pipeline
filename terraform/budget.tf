# A budget alarm is a smoke alarm, NOT a spending cap.
#
# AWS has no "stop charging me" switch. If a misconfigured Glue job runs in a loop,
# this emails you - it does not stop the job. That distinction has cost people real
# money and is worth knowing before you rely on it.
#
# Two notifications on purpose:
#   ACTUAL at 80%    - "you have already spent this"
#   FORECASTED at 100% - "at your current run rate you will blow the budget"
# The forecast one is the useful one; it fires days earlier. It needs a few days of
# history before AWS can forecast at all, so expect silence from it initially.
resource "aws_budgets_budget" "monthly" {
  name         = "${var.project_name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
