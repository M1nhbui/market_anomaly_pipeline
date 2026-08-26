# =============================================================================
# AWS Glue - serverless Spark
#
# A Glue job is: a Python file in S3, a worker count, and an IAM role. On each run
# AWS provisions a small Spark cluster, runs your script, and tears it down. You pay
# per DPU-second with a 1-minute minimum, which is why a job that processes 17 bars
# costs nearly the same as one that processes 17,000 - and why this pipeline batches
# hourly rather than every five minutes.
# =============================================================================

# -----------------------------------------------------------------------------
# IAM
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "glue_job_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = "${var.project_name}-glue-job"
  description        = "Glue ETL: read Bronze, write Silver. No delete on Bronze."
  assume_role_policy = data.aws_iam_policy_document.glue_job_trust.json
}

# Note the asymmetry, which IS the medallion architecture expressed in IAM:
#
#   Bronze : GetObject + ListBucket        (read only - no Put, no Delete)
#   Silver : Get + Put + Delete + List     (Delete is required; see below)
#
# s3:DeleteObject on Silver surprises people. An "overwrite" in S3 is not an
# in-place edit - Spark deletes the old objects in the target partitions and writes
# new ones. Without Delete the job fails partway through with an AccessDenied that
# arrives only at write time, after all the compute has been paid for.
#
# The absence of Delete on Bronze is the enforcement mechanism for immutability. It
# is not a convention we are trusting ourselves to honour; the permission does not
# exist, so a bug cannot violate it.
#
# ONE HARD-WON DETAIL: the scratch-space resource is "glue-temp*", NOT "glue-temp/*".
#
# S3 has no directories - it is a flat key-value store. Hadoop and Spark were written
# for filesystems that do, so the S3 connector fakes them by writing zero-byte marker
# objects named "<prefix>_$folder$". The first write into the scratch prefix therefore
# tries to PutObject on the key "glue-temp_$folder$", which is a SIBLING of
# "glue-temp/..." and does not match a "glue-temp/*" pattern. Result: a 403 that
# names a key you never asked anything to create.
#
# Dropping the slash covers both the markers and the real objects. The scripts/
# prefix keeps its own read-only statement, so the job still cannot overwrite its own
# source code - a property worth preserving deliberately rather than losing to a
# broader wildcard.
resource "aws_iam_role_policy" "glue_job" {
  name = "${var.project_name}-glue-job-policy"
  role = aws_iam_role.glue_job.id

  policy = templatefile("${path.module}/../iam_policies/glue_job.json", {
    artifacts_bucket_arn = aws_s3_bucket.layer["artifacts"].arn
    bronze_bucket_arn    = aws_s3_bucket.layer["bronze"].arn
    silver_bucket_arn    = aws_s3_bucket.layer["silver"].arn
    region               = var.aws_region
    account_id           = data.aws_caller_identity.current.account_id
  })
}

# -----------------------------------------------------------------------------
# Job script
# -----------------------------------------------------------------------------

# Glue reads its script from S3, so Terraform uploads it.
#
# `etag = filemd5(...)` is the same trick as source_code_hash on the Lambda: without
# it, Terraform sees an unchanged resource, skips the upload, and Glue keeps running
# your previous code while you wonder why the fix did nothing. This is one of the
# most common ways to lose half an hour on Glue.
resource "aws_s3_object" "bronze_to_silver_script" {
  bucket = aws_s3_bucket.layer["artifacts"].id
  key    = "scripts/bronze_to_silver.py"
  source = "${path.module}/../glue_jobs/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../glue_jobs/bronze_to_silver.py")
}

# -----------------------------------------------------------------------------
# Job definition
# -----------------------------------------------------------------------------

resource "aws_glue_job" "bronze_to_silver" {
  name        = "${var.project_name}-bronze-to-silver"
  description = "Parse, cast, drop unfinished bars, deduplicate. Bronze -> Silver."
  role_arn    = aws_iam_role.glue_job.arn

  # Glue 5.0 = Spark 3.5 + Python 3.11, matching requirements-dev.txt (pyspark 3.5.3).
  # Version parity between local tests and production is not optional: window
  # function semantics, the datetime parser, and Parquet writer defaults have all
  # changed between Spark majors. A local suite validating different behaviour than
  # production is worse than no suite, because it produces false confidence.
  glue_version = "5.0"

  # 2 x G.1X = the minimum for a Glue Spark job. G.1X is 4 vCPU / 16 GB per worker.
  #
  # More workers would not make this faster: at ~1,800 rows the job is dominated by
  # cluster startup, not compute. Adding workers would multiply cost while changing
  # runtime by roughly nothing. This is worth stating out loud because "throw more
  # workers at it" is the reflex, and here it is precisely wrong.
  worker_type       = "G.1X"
  number_of_workers = 2

  # Glue's DEFAULT TIMEOUT IS 2880 MINUTES - 48 hours.
  #
  # At 2 DPU x $0.44/DPU-hour that is roughly $42 for one runaway job. A hung job
  # with the default timeout is the single most expensive mistake available in this
  # project. 15 minutes is generous for a job that should finish in one or two.
  timeout = 15

  # No automatic retries. A retry on a genuinely broken job just doubles the bill,
  # and every failure we have seen so far has been deterministic (a bug), not
  # transient. Step Functions will own retry policy at slice 8, where it can
  # distinguish a Glue service error from our code failing.
  max_retries = 0

  # Two concurrent runs would race on the same Silver partitions. Because dynamic
  # partition overwrite is NOT atomic on plain Parquet, a race can leave a partition
  # half-replaced with no error reported. This is the concrete form of the
  # limitation that Apache Iceberg would fix (README section 19).
  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.layer["artifacts"].id}/${aws_s3_object.bronze_to_silver_script.key}"
  }

  default_arguments = {
    # Our own arguments, consumed by argparse in bronze_to_silver.py.
    "--bronze-path" = "s3://${aws_s3_bucket.layer["bronze"].id}/"
    "--silver-path" = "s3://${aws_s3_bucket.layer["silver"].id}/"

    # Glue's own arguments.
    "--TempDir"            = "s3://${aws_s3_bucket.layer["artifacts"].id}/glue-temp/"
    "--job-language"       = "python"
    "--enable-metrics"     = "true"
    "--enable-spark-ui"    = "false"
    "--job-bookmark-option" = "job-bookmark-disable"

    # Streams driver and executor logs to CloudWatch as the job runs rather than
    # only at the end. When a job hangs, this is the difference between watching
    # where it stopped and staring at an empty log group.
    "--enable-continuous-cloudwatch-log" = "true"
  }
}
