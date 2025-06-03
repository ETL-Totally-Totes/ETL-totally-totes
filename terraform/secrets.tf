data "aws_secretsmanager_secret_version" "database_secret" {
  secret_id = "database_secret"
}

