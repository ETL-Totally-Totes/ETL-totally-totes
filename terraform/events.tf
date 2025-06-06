#TODO: handle any changes to Lambda code after initial deployment
#CloudWatch event to run our lambda every 10 minutes


## TODO Change to trigger state machine when next lambda is implemented

resource "aws_cloudwatch_event_rule" "scheduler" {
  name_prefix = "extract_handler-scheduler"
  schedule_expression = "rate(10 minutes)"
}


resource "aws_lambda_permission" "allow_scheduler" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_handler.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}


resource "aws_cloudwatch_event_target" "extract_scheduler" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  arn       = aws_lambda_function.extract_handler.arn
}


#CloudWatch event to send us alerts about function errors?
resource "aws_cloudwatch_log_group" "extract_lambda_errors" {
  name = "Extract/errors.log"
}


resource "aws_cloudwatch_log_metric_filter" "extract_lambda_error" {
  name           = "ExtractError"
  pattern        = "error"
  log_group_name = aws_cloudwatch_log_group.extract_lambda_errors.name
  metric_transformation {
    name      = "ErrorAlert"
    namespace = "YourNamespace"
    value     = "1"
  }
}


# SNS alert
resource "aws_sns_topic" "extract_errors_alert" {
  name = "extract-error-alerts-topic"
}


resource "aws_sns_topic_subscription" "email_4" {
  topic_arn = aws_sns_topic.extract_errors_alert.arn
  protocol  = "email"
  endpoint  = "selva.pla.roj@gmail.com"
}


resource "aws_sns_topic_subscription" "email_1" {
  topic_arn = aws_sns_topic.extract_errors_alert.arn
  protocol  = "email"
  endpoint  = "smorton@gmx.co.uk"
}



resource "aws_sns_topic_subscription" "email_2" {
  topic_arn = aws_sns_topic.extract_errors_alert.arn
  protocol  = "email"
  endpoint  = "george.krokos1@gmail.com"
}


resource "aws_sns_topic_subscription" "email_3" {
  topic_arn = aws_sns_topic.extract_errors_alert.arn
  protocol  = "email"
  endpoint  = "abbyadjei9@gmail.com"
}


resource "aws_sns_topic_subscription" "email_5" {
  topic_arn = aws_sns_topic.extract_errors_alert.arn
  protocol  = "email"
  endpoint  = "dalewithvan@gmail.com"
}

# ------------------------------
# Tranform lambda tf code
# ------------------------------

# This will trigget our transform lambda if there is any change in the s3 ingestion bucket DELETE IF YOU WANT!
resource "aws_lambda_permission" "allow_s3_to_invoke_transform" {
  statement_id  = "AllowS3ToInvokeTransform"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transform_handler.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.ingestion_bucket.arn
  source_account = data.aws_caller_identity.current.account_id 
}

# Configure S3 Event Notification to trigger the Transform Lambda
resource "aws_s3_bucket_notification" "trigger_transform_on_extract_completion" {
  bucket = aws_s3_bucket.ingestion_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.transform_handler.arn
    events              = ["s3:ObjectCreated:*"] 
  }

  # Ensure the Lambda permission is created before the S3 notification
  depends_on = [aws_lambda_permission.allow_s3_to_invoke_transform]
}

