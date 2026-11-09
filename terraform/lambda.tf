locals {
  ingestion_function_name = "${var.project_name}-ingestion"
  dq_gate_function_name   = "${var.project_name}-dq-gate"

  # config/symbols.json is the single source of truth for the basket.
  #
  # Terraform reads it here and feeds it to the Lambda's environment. Later it will
  # also feed the Athena partition-projection enum from this same value. That is the
  # point: the symbol list exists in exactly one file, so the Lambda and the query
  # layer cannot drift apart. The README calls out that drift as a real bug in the
  # project this one is modelled on.
  symbols_config = jsondecode(file("${path.module}/../config/symbols.json"))
  active_symbols = local.symbols_config.active
}

# Lambda wants a .zip. Since there are no third-party dependencies, zipping the
# source directory is the entire build step.
#
# output_base64sha256 below is what makes redeploys work: it changes whenever the
# source changes, so `terraform apply` uploads new code. Without it Terraform sees
# an unchanged function resource and skips the upload, and you spend twenty minutes
# wondering why your edit had no effect. This is an extremely common trap.
data "archive_file" "ingestion" {
  type        = "zip"
  source_dir  = "${path.module}/../lambdas/ingestion"
  output_path = "${path.module}/build/ingestion.zip"
}

resource "aws_lambda_function" "ingestion" {
  function_name = local.ingestion_function_name
  description   = "Fetch Binance klines and write raw JSON to Bronze. No transformation."

  role    = aws_iam_role.ingestion_lambda.arn
  handler = "lambda_function.lambda_handler" # <file name>.<function name>
  runtime = "python3.12"

  filename         = data.archive_file.ingestion.output_path
  source_code_hash = data.archive_file.ingestion.output_base64sha256

  # 60s is generous. 17 symbols at roughly 200ms each is ~4s, and the retry/backoff
  # path is the only thing that could approach the ceiling. Timeout too low and a
  # slow network kills the run; too high and a hung request burns billed time doing
  # nothing. Lambda bills duration, so this is a real cost knob.
  timeout = 60

  # 256MB. Lambda allocates CPU proportionally to memory, so raising this can make
  # a job cheaper by finishing faster. This workload is network-bound, not CPU-bound,
  # so extra memory buys nothing - 128MB would also work. We will have the measured
  # duration after the first invocation and can tune it against reality.
  memory_size = 256

  environment {
    variables = {
      BRONZE_BUCKET = aws_s3_bucket.layer["bronze"].id
      SOURCE_HOST   = "https://${local.symbols_config.source_host}"
      INTERVAL      = local.symbols_config.interval
      SYMBOLS       = jsonencode(local.active_symbols)
      BAR_LIMIT     = "15"
    }
  }

  # Without this, Lambda creates the log group implicitly with never-expire
  # retention, and CloudWatch Logs storage accumulates forever. Declaring it lets us
  # set retention and lets Terraform delete it on destroy.
  depends_on = [aws_cloudwatch_log_group.ingestion]
}

resource "aws_cloudwatch_log_group" "ingestion" {
  name              = "/aws/lambda/${local.ingestion_function_name}"
  retention_in_days = 14
}

# -----------------------------------------------------------------------------
# DQ gate
# -----------------------------------------------------------------------------

data "archive_file" "dq_gate" {
  type        = "zip"
  source_dir  = "${path.module}/../lambdas/dq_gate"
  output_path = "${path.module}/build/dq_gate.zip"
}

resource "aws_lambda_function" "dq_gate" {
  function_name = local.dq_gate_function_name
  description   = "Query Silver via Athena, evaluate quality checks, return quality_passed."

  role    = aws_iam_role.dq_gate.arn
  handler = "lambda_function.lambda_handler"
  runtime = "python3.12"

  filename         = data.archive_file.dq_gate.output_path
  source_code_hash = data.archive_file.dq_gate.output_base64sha256

  # Two Athena queries, each polled once a second. Athena on a few tens of thousands
  # of Parquet rows typically answers in 2-5 seconds, so ~15 s is the realistic case.
  # 120 s leaves room for a queued query without the Lambda being killed mid-poll -
  # which would lose the structured result and, worse, leave Step Functions with no
  # quality_passed value at all.
  timeout = 120

  # Almost entirely waiting on Athena. Memory buys CPU, and there is no CPU work here.
  memory_size = 256

  environment {
    variables = {
      ATHENA_DATABASE   = aws_glue_catalog_database.silver.name
      ATHENA_WORKGROUP  = aws_athena_workgroup.main.name
      SILVER_TABLE      = "${aws_glue_catalog_database.silver.name}.${aws_glue_catalog_table.clean_bars.name}"
      DQ_RESULTS_BUCKET = aws_s3_bucket.layer["gold"].id
      DQ_RESULTS_PREFIX = "dq_results"

      # From config/symbols.json, same as the ingestion Lambda. The expected row
      # count is derived from this rather than hardcoded, so it cannot rot when the
      # basket changes.
      SYMBOL_COUNT = tostring(length(local.active_symbols))

      DQ_WINDOW_HOURS = tostring(var.dq_window_hours)

      # Thresholds as variables, not constants in code, so tuning them is a
      # reviewable Terraform diff rather than a Lambda redeploy. Every default is
      # traceable to a measurement - see variables.tf.
      DQ_MIN_ROW_RATIO         = tostring(var.dq_min_row_ratio)
      DQ_MAX_EMPTY_BAR_PCT     = tostring(var.dq_max_empty_bar_pct)
      DQ_MAX_GAP_PCT           = tostring(var.dq_max_gap_pct)
      DQ_MAX_FRESHNESS_MINUTES = tostring(var.dq_max_freshness_minutes)
    }
  }

  depends_on = [aws_cloudwatch_log_group.dq_gate]
}

resource "aws_cloudwatch_log_group" "dq_gate" {
  name              = "/aws/lambda/${local.dq_gate_function_name}"
  retention_in_days = 14
}
