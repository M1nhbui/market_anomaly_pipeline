variable "project_name" {
  description = "Short name used as a prefix for every resource and as the cost-allocation tag value."
  type        = string
  default     = "crypto-anomaly"

  validation {
    condition     = can(regex("^[a-z0-9-]{3,20}$", var.project_name))
    error_message = "project_name must be 3-20 chars, lowercase letters, digits, and hyphens only (S3 bucket naming rules)."
  }
}

variable "aws_region" {
  description = "AWS region. us-east-1: cheapest, full Glue/Athena coverage, error messages match the docs."
  type        = string
  default     = "us-east-1"
}

variable "alert_email" {
  description = "Email for budget alerts (and later, SNS pipeline alerts). No default on purpose - this must be a conscious choice, not something you inherit from a template."
  type        = string

  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.alert_email))
    error_message = "alert_email must look like an email address."
  }
}

variable "budget_limit_usd" {
  description = "Monthly budget ceiling. Alerts fire at 80% actual and 100% forecast. This is a smoke alarm, not a spending cap - AWS will NOT stop charging you when it trips."
  type        = number
  default     = 10
}

variable "ingestion_schedule_enabled" {
  description = "Master switch for the 5-minute ingestion schedule. Ingestion is cheap (Lambda's always-free tier should cover ~8,600 invocations/month), so this stays on. The expensive Glue schedule is a separate switch added at slice 8."
  type        = bool
  default     = true
}

variable "athena_results_retention_days" {
  description = "Athena writes a result file to S3 for every query, including failed ones. Left alone this grows forever and quietly costs money. 30 days is well past any debugging window."
  type        = number
  default     = 30
}
