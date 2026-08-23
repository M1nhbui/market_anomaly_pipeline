locals {
  ingestion_function_name = "${var.project_name}-ingestion"

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
