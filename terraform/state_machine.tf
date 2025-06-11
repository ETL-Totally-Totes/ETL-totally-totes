resource "aws_sfn_state_machine" "etl_totally_totes_workflow" {
  name     = var.state_machine_name
  role_arn = aws_iam_role.state_role.arn
  publish  = true
  # logging_configuration {
  #   include_execution_data = true
  #   level = "ALL"
  #   log_destination = "${aws_cloudwatch_log_group.etl_totally_totes_workflow_logs.arn}:*"
  # }
  type     = "STANDARD"
  definition = templatefile("${path.module}/state-machine.asl.json", {
    extract_arn = aws_lambda_function.extract_handler.arn,
    transform_arn = aws_lambda_function.transform_handler.arn,
    load_arn = aws_lambda_function.load_handler.arn
    }
  )

}


resource "aws_cloudwatch_log_group" "etl_totally_totes_workflow_logs" {
  name = "etl_totally_totes_workflow_logs"
}
