#CloudWatch event to run our extract_lambda every 10 minutes

resource "aws_lambda_permission" "allow_scheduler" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_handler.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_cloudwatch_event_rule" "scheduler" {
  #TODO: this should set up a scheduler that will trigger the Lambda every 5 minutes
  # Careful! other things may need to be set up as well
  name_prefix = "extract_handler-scheduler"
  schedule_expression = "rate(10 minutes)"
}

resource "aws_cloudwatch_event_target" "yada" {
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

#SNS alert

resource "aws_sns_topic" "extract_errors_alert" {
  name = "extract-error-alerts-topic"
}

# resource "aws_sqs_queue" "extract_errors_queue" {
#   name   = "extract-error-alerts-queue"
#   policy = data.aws_iam_policy_document.lambda_sns_topic_policy_attachment.json
# }

resource "aws_sns_topic_subscription" "extract_errors_alert_email_target" {
  topic_arn = aws_sns_topic.extract_errors_alert.arn   #aws_sns_topic.user_updates.arn
  protocol  = "email"
  endpoint  = "selva.pla.roj@gmail.com"
}

# data "aws_iam_policy_document" "sqs_queue_policy" {
  
#   statement {
#     sid    = "extract_errors_alert_email_target"
#     effect = "Allow"

#     principals {
#       type        = "Service"
#       identifiers = ["sns.amazonaws.com"]
#     }

#     actions = [
#       "SQS:SendMessage",
#     ]

#     resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]


#   }
# }
