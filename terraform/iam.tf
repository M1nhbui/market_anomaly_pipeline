# =============================================================================
# IAM
#
# Every IAM role is TWO separate policies, and conflating them is the single most
# common source of confusing AWS errors:
#
#   1. TRUST POLICY (assume_role_policy) - WHO is allowed to become this role.
#      Get this wrong and the service cannot even pick up the role. The error is a
#      generic AccessDenied that never mentions trust, which is why it eats hours.
#
#   2. PERMISSION POLICY - WHAT the role may do once it has become the role.
#      Get this wrong and you get a specific, readable AccessDenied naming the
#      action and resource. These are the easy ones.
#
# A Lambda has no password or key. At invocation, the Lambda service assumes the
# role attached to the function and hands the resulting temporary credentials to
# your code as environment variables. boto3 finds them automatically. That is why
# lambda_function.py contains no credentials anywhere.
# =============================================================================

# -----------------------------------------------------------------------------
# Ingestion Lambda role
# -----------------------------------------------------------------------------

# The trust policy. "lambda.amazonaws.com may assume this role." Nothing else can -
# not you, not another service, not another account.
data "aws_iam_policy_document" "ingestion_lambda_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ingestion_lambda" {
  name               = "${var.project_name}-ingestion-lambda"
  description        = "Ingestion Lambda: write Bronze objects and its own logs. Nothing else."
  assume_role_policy = data.aws_iam_policy_document.ingestion_lambda_trust.json
}

# The permission policy, authored as a standalone readable JSON file under
# iam_policies/ and interpolated here.
#
# Why a separate file rather than a Terraform data source: the JSON is the exact
# artifact AWS evaluates, so it can be pasted into the IAM policy simulator, diffed
# against what the console shows, and read by someone who does not know HCL. The
# cost is losing Terraform's compile-time validation of the document.
resource "aws_iam_role_policy" "ingestion_lambda" {
  name = "${var.project_name}-ingestion-lambda-policy"
  role = aws_iam_role.ingestion_lambda.id

  policy = templatefile("${path.module}/../iam_policies/ingestion_lambda.json", {
    bronze_bucket_arn = aws_s3_bucket.layer["bronze"].arn
    region            = var.aws_region
    account_id        = data.aws_caller_identity.current.account_id
    function_name     = local.ingestion_function_name
  })
}

# -----------------------------------------------------------------------------
# DQ gate Lambda role
#
# README §13: "The DQ Gate role is most often under-scoped: Athena needs catalog read
# and results-bucket write, not just 'read Silver'."
#
# That is worth spelling out, because the mental model people arrive with is wrong.
# Athena is not a service that reads your data on its own behalf - it runs the query
# USING THE CALLER'S CREDENTIALS. So this role needs, in full:
#
#   1. athena:*QueryExecution      - obvious, and the only part most people grant
#   2. athena:GetWorkGroup         - required because our workgroup sets
#                                    enforce_workgroup_configuration = true; without
#                                    it every query fails before it starts
#   3. glue:Get{Database,Table,Partitions} - Athena reads the table definition from
#                                    the Catalog, as us
#   4. s3:GetObject on SILVER      - Athena reads the Parquet, as us
#   5. s3:PutObject on ATHENA-RESULTS - Athena writes the result set to S3 before
#                                    handing it back, as us. Forgetting this produces
#                                    a query that fails at the very end, after the
#                                    scan has already been billed.
#
# Items 2 and 5 are the ones that get missed. Both fail late and unhelpfully.
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "dq_gate_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "dq_gate" {
  name               = "${var.project_name}-dq-gate"
  description        = "DQ gate Lambda: query Silver via Athena, write results to Gold."
  assume_role_policy = data.aws_iam_policy_document.dq_gate_trust.json
}

resource "aws_iam_role_policy" "dq_gate" {
  name = "${var.project_name}-dq-gate-policy"
  role = aws_iam_role.dq_gate.id

  policy = templatefile("${path.module}/../iam_policies/dq_gate_lambda.json", {
    workgroup                 = aws_athena_workgroup.main.name
    silver_database           = aws_glue_catalog_database.silver.name
    silver_bucket_arn         = aws_s3_bucket.layer["silver"].arn
    athena_results_bucket_arn = aws_s3_bucket.layer["athena-results"].arn
    gold_bucket_arn           = aws_s3_bucket.layer["gold"].arn
    region                    = var.aws_region
    account_id                = data.aws_caller_identity.current.account_id
    function_name             = local.dq_gate_function_name
  })
}

# Deliberately NOT granted to the DQ gate:
#   s3:PutObject on Silver  - it inspects, it never writes data
#   s3:*      on Bronze     - it has no business there at all
#   s3:PutObject on Gold outside dq_results/ - it must not be able to touch
#                             anomaly_events, which is the Glue job's output

# -----------------------------------------------------------------------------
# Notes on what is deliberately NOT granted to the ingestion role
# -----------------------------------------------------------------------------
#
# s3:GetObject     - ingestion never reads. If it did, that would be a bug.
# s3:DeleteObject  - Bronze is immutable. Nothing should ever delete from it, so
#                    the permission does not exist to be misused.
# s3:ListBucket    - not needed for PutObject. Note the ARN shape difference:
#                    ListBucket acts on the BUCKET arn, object actions act on
#                    "<bucket-arn>/*". Mixing those two up is the most common
#                    S3 policy mistake and produces an AccessDenied that looks
#                    like the policy should have worked.
# s3:* on *        - never. The whole point of five separate roles is that a bug or
#                    compromise in one component cannot touch the others' data.
