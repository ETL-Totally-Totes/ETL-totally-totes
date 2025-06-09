# resource "aws_sfn_state_machine" "etl_totally_totes_workflow" {
#   name     = var.state_machine_name
#   role_arn = aws_iam_role.state_role.arn
#   publish  = true
#   type     = "EXPRESS"

#   definition = ##THIS WHOLE BLOCK WILL BE DEFINED AFTER WE USED STEP FUNCTIONS
# {
#   "Comment": "A Hello World example of the Amazon States Language using an AWS Lambda Function",
#   "StartAt": "HelloWorld",
#   "States": {
#     "HelloWorld": {
#       "Type": "Task",
#       "Resource": "${aws_lambda_function.lambda.arn}",
#       "End": true
#     }
#   }
# }

# }