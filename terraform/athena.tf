# =============================================================================
# Amazon Athena
#
# Athena is serverless SQL over files in S3. There is no database and no cluster:
# each query reads the Catalog to learn where the files are, reads those files, and
# bills you per BYTE SCANNED (~$5 per TB, minimum 10 MB per query).
#
# That pricing model drives every storage decision already made:
#   - Parquet is columnar, so `SELECT close` reads only the close column's bytes.
#     The same query on JSON would read every row in full.
#   - Partitioning by symbol/date lets Athena skip whole files when the WHERE clause
#     filters on them. A query without a partition filter scans everything.
#   - Snappy compression means fewer bytes to scan, which is directly fewer dollars.
#
# Cost control here is not about tuning the engine. It is about how the data was
# written, which is why this file is short and glue.tf is long.
# =============================================================================

resource "aws_athena_workgroup" "main" {
  name        = var.project_name
  description = "Query workgroup for the crypto anomaly pipeline."

  configuration {
    # Force every query to use THIS workgroup's settings, ignoring whatever a client
    # (console, JDBC driver, boto3) tries to specify for itself. Without this, a
    # client can override the result location and the scan limit, which makes both
    # the cost guard and the output-location lifecycle rule optional in practice.
    enforce_workgroup_configuration = true

    publish_cloudwatch_metrics_enabled = true

    # HARD COST GUARD. Any single query projected to scan more than this is
    # cancelled before it runs, not billed for the overage.
    #
    # 1 GB is enormous relative to Silver's current 195 KiB - it will never fire in
    # normal use. That is the point: it is a circuit breaker for the accidental
    # cross join or the missing partition filter on a table that has grown, not a
    # limit meant to be felt day to day. AWS enforces a 10 MB floor on this setting.
    bytes_scanned_cutoff_per_query = 1073741824 # 1 GiB

    result_configuration {
      # Every query - including failed ones, and every DQ-gate check later - writes
      # a result file here. The 30-day lifecycle rule on this bucket (s3.tf) is what
      # stops that from accumulating forever.
      output_location = "s3://${aws_s3_bucket.layer["athena-results"].id}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  # Lets `terraform destroy` remove the workgroup even after queries have run
  # against it. Without this, teardown fails on a workgroup with query history.
  force_destroy = true
}
