terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # Zips the Lambda source directory at plan time. Added at slice 1, so
    # `terraform init` must be re-run to download it.
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  # State is LOCAL for v1, deliberately.
  #
  # An S3 backend is the "correct" answer for a team, but it has a chicken-and-egg
  # problem for a solo project: the bucket holding the state must exist before
  # Terraform can use it, so you end up bootstrapping it by hand or with a second
  # Terraform project. Not worth it while you are the only operator.
  #
  # terraform.tfstate is gitignored. It can contain resource metadata you would not
  # want in a public repo, so that is not optional.
  #
  # To migrate later:
  #   backend "s3" {
  #     bucket = "<your-state-bucket>"
  #     key    = "crypto-anomaly/terraform.tfstate"
  #     region = "us-east-1"
  #   }
  # then run: terraform init -migrate-state
}

provider "aws" {
  region = var.aws_region

  # Applied to every taggable resource this provider creates.
  #
  # These are not decoration. Cost Explorer can only break costs down by tag if the
  # tag exists on the resource AND has been activated as a cost-allocation tag in
  # the Billing console. Tagging from the first apply means the cost data is already
  # sliceable when we go to measure it, instead of us wishing we had tagged sooner.
  default_tags {
    tags = {
      Project   = var.project_name
      ManagedBy = "terraform"
    }
  }
}

# Reads the account ID of whoever is running terraform. Used to make bucket names
# globally unique without introducing a random_id resource whose value we would then
# have to keep in state forever.
data "aws_caller_identity" "current" {}
