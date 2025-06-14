# ---------------
# Lambda IAM Role
# ---------------
# Define
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
  name               = "lambda_role"
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
      "s3:Put*", "s3:Get*", "s3:List*" #TODO: ADD MORE - 2/6 added more actions
    ]
    resources = [aws_s3_bucket.ingestion_bucket.arn, "${aws_s3_bucket.ingestion_bucket.arn}/*",
      aws_s3_bucket.transformation_bucket.arn, "${aws_s3_bucket.transformation_bucket.arn}/*"
    ]
  }
} #TODO: ADD MORE BUCKETS WHEN WE CREATE THEM
# Create
resource "aws_iam_policy" "lambda_s3_policy" {
  name   = "lambda_s3_policy"
  policy = data.aws_iam_policy_document.s3_data_policy_doc.json
}
# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
  role       = aws_iam_role.lambda_role.name #TODO: attach the s3 write policy to the lambda role
  policy_arn = aws_iam_policy.lambda_s3_policy.arn
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
  statement {
    #this statement should give permission to the transform lambda describe Log Streams and groups in the extract lambda
    effect = "Allow"
    actions = [
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
      "logs:GetLogEvents"
    ]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${aws_lambda_function.extract_handler.function_name}:*"]
  }
}


# Create
resource "aws_iam_policy" "cw_policy" {
  #TODO: use the policy document defined above
  policy = data.aws_iam_policy_document.cw_document.json
}


# Attach
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  #TODO: attach the cw policy to the lambda role
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}
# ------------------------------
# Lambda IAM Policy for SNS
# ------------------------------
# Define
data "aws_iam_policy_document" "sns_topic_document" {
  statement {
    actions = [
      "SNS:Subscribe",
      "SNS:SetTopicAttributes",
      "SNS:RemovePermission",
      "SNS:Receive",
      "SNS:Publish",
      "SNS:ListSubscriptionsByTopic",
      "SNS:GetTopicAttributes",
      "SNS:DeleteTopic",
      "SNS:AddPermission",
    ]

    effect = "Allow"

    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }
}

# -----------------
# SNS
# -----------------
# Create
resource "aws_iam_policy" "sns_topic_policy" {
  #TODO: use the policy document defined above
  policy = data.aws_iam_policy_document.sns_topic_document.json
  #data.aws_iam_policy_document.sns_topic_document.json
}
# Attach
resource "aws_iam_role_policy_attachment" "lambda_sns_topic_policy_attachment" {
  #TODO: attach the cw policy to the lambda role
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}

data "aws_iam_policy_document" "sqs_queue_policy" {
  statement {
    sid    = "extract_errors_alert_email_target"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["sns.amazonaws.com"]
    }
    actions = [
      "SQS:SendMessage",
    ]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }
}


# -----------------
# state machine
# -----------------

# Create an IAM role for the Step Functions state machine

data "aws_iam_policy_document" "state_policy" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "state_role" {
  name               = "state_role"
  assume_role_policy = data.aws_iam_policy_document.state_policy.json
}

data "aws_iam_policy_document" "state_machine_role_policy" {
  statement {
    effect = "Allow"
    actions = [
      "cloudwatch:PutMetricData",
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:PutLogEvents",
      "logs:CreateLogStream",
      "logs:DescribeLogGroups"
    ]
    resources = ["*"]
  }
  statement {
    effect = "Allow"
    actions = [
    # "logs:CreateLogGroup",
    "logs:CreateLogStream",
    "logs:PutLogEvents",
    "logs:DescribeLogGroups",
    "logs:DescribeLogStreams",
    ]
    resources = [aws_cloudwatch_log_group.etl_totally_totes_workflow_logs.arn]
  }
  statement {
    effect = "Allow"
    actions = [
      "lambda:InvokeFunction"
    ]
    resources = [ 
      "${aws_lambda_function.extract_handler.arn}",
      "${aws_lambda_function.transform_handler.arn}",
      "${aws_lambda_function.load_handler.arn}"
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "lambda:InvokeFunction"
    ]
    resources = [
      "${aws_lambda_function.extract_handler.arn}:*",
      "${aws_lambda_function.transform_handler.arn}:*",
      "${aws_lambda_function.load_handler.arn}:*"
    ]
  }
}

resource "aws_iam_policy" "state_machine_role_policy" {
  name   = "state_machine_role_policy"
  policy = data.aws_iam_policy_document.state_machine_role_policy.json
}
# Attach
resource "aws_iam_role_policy_attachment" "state_machine_attachment" {
  role       = aws_iam_role.state_role.name #TODO: attach the s3 write policy to the lambda role
  policy_arn = aws_iam_policy.state_machine_role_policy.arn
}

#TODO:add other lambdas
# Create
