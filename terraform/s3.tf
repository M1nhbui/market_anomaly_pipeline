locals {
  # Four buckets rather than one bucket with bronze/ silver/ gold/ prefixes.
  #
  # Why: IAM. The Glue job needs write on Silver but must NOT have write on Bronze -
  # Bronze immutability is the whole point of the medallion pattern's first layer.
  # With separate buckets that is a bucket ARN in a policy. With one bucket it is a
  # prefix condition, which is easy to get subtly wrong and hard to read in a review.
  # The cost is identical; S3 charges for bytes, not buckets.
  buckets = {
    bronze         = "Raw Binance JSON - exactly as received. Immutable."
    silver         = "Cleaned - typed - deduplicated bars as Parquet."
    gold           = "Analytics tables and anomaly events as Parquet."
    athena-results = "Athena query output. Ephemeral - expired on a lifecycle rule."
  }

  bucket_names = {
    for k, _ in local.buckets :
    k => "${var.project_name}-${k}-${data.aws_caller_identity.current.account_id}"
  }
}

# S3 bucket names are globally unique across every AWS account on earth, so
# "crypto-anomaly-bronze" was taken years ago. Suffixing the account ID makes the
# name unique and deterministic - re-running terraform always produces the same name.
resource "aws_s3_bucket" "layer" {
  for_each = local.buckets

  bucket = local.bucket_names[each.key]

  tags = {
    Layer   = each.key
    Purpose = each.value
  }
}

# Default-deny public access. S3 has had public-by-accident incidents for a decade;
# this is four separate flags because AWS added them at different times and each
# closes a different hole. Always set all four.
resource "aws_s3_bucket_public_access_block" "layer" {
  for_each = aws_s3_bucket.layer

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# SSE-S3 (AES256) encryption at rest. Free, and it means "is the data encrypted?"
# has a boring yes answer. KMS would give you per-key audit trails and cross-account
# control, but adds a per-request charge and one more thing every IAM role needs
# permission on. Not worth it at this scope.
resource "aws_s3_bucket_server_side_encryption_configuration" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Versioning on Bronze ONLY.
#
# Bronze is the "we can always reprocess" guarantee, so protecting it from an
# accidental overwrite or delete is the point of the layer. Silver and Gold are
# both derived - if they are wrong we rebuild them from Bronze, and versioning them
# would just accumulate every superseded Parquet file from every job run forever.
resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.layer["bronze"].id

  versioning_configuration {
    status = "Enabled"
  }
}

# Housekeeping.
#
# The abort-incomplete-multipart rule matters more than it looks: a failed Spark
# write can leave multipart upload parts that you are billed for but cannot see with
# `aws s3 ls`. This is a classic mystery-charge source. Seven days is generous.
resource "aws_s3_bucket_lifecycle_configuration" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  # Athena results are disposable. Every query - including every failed one and
  # every DQ-gate check - writes a file here. Expire them.
  dynamic "rule" {
    for_each = each.key == "athena-results" ? [1] : []

    content {
      id     = "expire-query-results"
      status = "Enabled"

      filter {}

      expiration {
        days = var.athena_results_retention_days
      }
    }
  }

  # Bronze versioning creates noncurrent versions on every overwrite. Without this
  # they are kept and billed forever.
  dynamic "rule" {
    for_each = each.key == "bronze" ? [1] : []

    content {
      id     = "expire-old-bronze-versions"
      status = "Enabled"

      filter {}

      noncurrent_version_expiration {
        noncurrent_days = 30
      }
    }
  }

  depends_on = [aws_s3_bucket_versioning.bronze]
}
