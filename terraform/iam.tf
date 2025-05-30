# ---------------
# Lambda IAM Role
# ---------------

# Define
data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# Create
resource "aws_iam_role" "lambda_role" {
  name = "lambda_role"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

# ------------------------------
# Lambda IAM Policy for S3
# ------------------------------

# Define
data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {
    effect = "Allow"
    actions = [
        "s3:PutObject" #TODO: ADD MORE ACTIONS
      ]
    resources = [ aws_s3_bucket.ingestion_bucket.arn,"${aws_s3_bucket.ingestion_bucket.arn}/*" ]
  }
} #TODO: ADD MORE BUCKETS WHEN WE CREATE THEM

# Create
resource "aws_iam_policy" "lambda_s3_policy" {
  name = "lambda_s3_policy"
  policy      = data.aws_iam_policy_document.s3_data_policy_doc.json 
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
    role = aws_iam_role.lambda_role.name #TODO: attach the s3 write policy to the lambda role
    policy_arn =  aws_iam_policy.lambda_s3_policy.arn

}

# ------------------------------
# Lambda IAM Policy for CloudWatch
# ------------------------------

# Define
data "aws_iam_policy_document" "cw_document" {
  statement {
    effect = "Allow"
    actions = [
        "logs:CreateLogGroup"
      ]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }

  statement {
    #this statement should give permission to create Log Streams and put Log Events in the lambda's own Log Group
    effect = "Allow"
    actions = [
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogGroups"
      ]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }
}

# Create
resource "aws_iam_policy" "cw_policy" {
  #TODO: use the policy document defined above
  policy      = data.aws_iam_policy_document.cw_document.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  #TODO: attach the cw policy to the lambda role
  role = aws_iam_role.lambda_role.name 
  policy_arn =  aws_iam_policy.cw_policy.arn
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}