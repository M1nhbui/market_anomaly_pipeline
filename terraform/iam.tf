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
# Notes on what is deliberately NOT granted to this role
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
