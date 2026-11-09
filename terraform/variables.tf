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

# -----------------------------------------------------------------------------
# DQ gate thresholds.
#
# Every default below traces to a measurement in METRICS.md rather than a guess.
# Kept as Terraform variables so tuning one is a reviewable diff, not a code change.
# -----------------------------------------------------------------------------

variable "dq_window_hours" {
  description = "How much recent Silver the gate evaluates. 24h rather than the 3h transform lookback: ratio checks need a sample large enough to be stable. Over 3 hours SHIBUSDT sees ~180 bars, so one quiet stretch swings its empty-bar ratio by several points and the gate becomes a coin flip."
  type        = number
  default     = 24
}

variable "dq_min_row_ratio" {
  description = "Fraction of theoretically-possible bars that must be present (symbols x 60 x window_hours). 0.70 tolerates a symbol being briefly unavailable or a couple of missed ingestion cycles without blocking."
  type        = number
  default     = 0.70
}

variable "dq_max_empty_bar_pct" {
  description = "Untraded-minute tolerance, WARN severity. Measured worst case is SHIBUSDT at 1.89% (M-039) over 10.6 hours, which did NOT include a full overnight cycle when liquidity is thinnest. 5% leaves headroom while still catching a feed returning empty bars wholesale."
  type        = number
  default     = 5.0
}

variable "dq_max_gap_pct" {
  description = "Sequence-gap tolerance, WARN severity. Measured: ZERO gaps across 23,327 bars over 16 days (M-022, M-028). Not calibrated to observed noise - there is none - but to avoid blocking on a single missed ingestion cycle that the 15-bar overlap would normally backfill."
  type        = number
  default     = 0.5
}

variable "dq_max_freshness_minutes" {
  description = "Newest-bar age tolerance, FAIL severity. Measured structural lag is 4m23s (M-025): the 5-minute ingestion interval plus the deliberate exclusion of the in-progress candle. README §11 specifies 'within one interval of now', which at 1-minute bars would fail on EVERY run. 15 minutes covers the structural lag plus two missed cycles."
  type        = number
  default     = 15
}

variable "zscore_window_bars" {
  description = <<-EOT
    Trailing window, in bars, for the Gold anomaly z-score baseline.

    A MODELLING CHOICE, not a fact. Shorter reacts faster and is noisier; longer is
    smoother and slower to respond. 60 one-minute bars = one hour of baseline.

    Deliberately different from Silver's 20-bar rolling_vol window: that one is a
    feature describing recent volatility, this one is a baseline for judging whether
    a single observation is unusual, and the second job wants a more stable estimate.
  EOT
  type        = number
  default     = 60
}

variable "zscore_threshold" {
  description = <<-EOT
    |z| above which a bar is emitted as an anomaly event.

    3.0 implies ~0.27% of observations under a NORMAL distribution. Minute-level
    crypto returns are nothing like normal - the largest move measured so far is
    32.5 standard deviations (METRICS.md M-030), where normal theory would put the
    maximum of 23,000 draws near 4. Expect the real firing rate to be far above
    0.27%, and expect to report the MEASURED rate rather than the theoretical one.
  EOT
  type        = number
  default     = 3.0
}

variable "glue_job_timeout_minutes" {
  description = <<-EOT
    Hard ceiling on a Glue job run, in minutes.

    Glue's own default is 2880 (48 hours), which at 2 DPU is roughly $42 for a single
    hung job - the most expensive mistake available in this project.

    Raised from 15 to 25 ahead of the 17-symbol expansion. The projection said ~8
    minutes, but that came from a runtime model the measurements have already
    contradicted twice (M-029, M-034), so the honest position is that we do not know
    what it will take. A higher ceiling costs nothing unless a job actually runs long,
    while a ceiling that is too low breaks the pipeline and hides the real number.
    Measure first, tune after.
  EOT
  type        = number
  default     = 25
}

variable "bronze_to_silver_lookback_hours" {
  description = <<-EOT
    How far back the Bronze->Silver job looks for new data.

    Any DATE with bars in this window is rewritten IN FULL, because dynamic partition
    overwrite replaces whole (symbol, date) partitions - writing a partial day would
    destroy the rest of it. The job also reads one extra prior day for feature
    warm-up and discards it before writing.

    3 hours means at most two target dates plus one warm-up day = three days of
    Bronze per run, bounded forever.

    Set to 0 for a FULL REPROCESS of all history. That is the recovery path: Bronze
    is immutable, so rebuilding Silver from scratch is always available and is how
    you would recover from a bad transform.
  EOT
  type        = number
  default     = 3
}

variable "projection_start_date" {
  description = "Earliest date Athena's partition projection will consider, yyyy-MM-dd. Set it to roughly when ingestion began. Too early wastes query-planning time on paths that never existed; too late makes real data invisible with no error."
  type        = string
  default     = "2026-08-23"
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
