output "bucket_names" {
  description = "Resolved S3 bucket names. Later slices reference these; copy them when running aws-cli checks by hand."
  value       = local.bucket_names
}

output "aws_region" {
  value = var.aws_region
}

output "account_id" {
  description = "Useful for reading IAM errors, which quote account IDs rather than names."
  value       = data.aws_caller_identity.current.account_id
}

output "ingestion_function_name" {
  value = aws_lambda_function.ingestion.function_name
}

output "active_symbols" {
  description = "Resolved from config/symbols.json - confirms Terraform read the file you think it did."
  value       = local.active_symbols
}

output "bronze_to_silver_job_name" {
  value = aws_glue_job.bronze_to_silver.name
}

output "silver_table" {
  description = "Fully qualified table name for Athena queries."
  value       = "${aws_glue_catalog_database.silver.name}.${aws_glue_catalog_table.clean_bars.name}"
}

output "athena_workgroup" {
  value = aws_athena_workgroup.main.name
}

output "silver_to_gold_job_name" {
  value = aws_glue_job.silver_to_gold.name
}

output "anomaly_events_table" {
  value = "${aws_glue_catalog_database.gold.name}.${aws_glue_catalog_table.anomaly_events.name}"
}

output "dq_gate_function_name" {
  value = aws_lambda_function.dq_gate.function_name
}

output "dq_results_table" {
  value = "${aws_glue_catalog_database.gold.name}.${aws_glue_catalog_table.dq_results.name}"
}

output "transform_chain_arn" {
  description = "Step Functions state machine for Silver -> gate -> Gold. Start a run with: aws stepfunctions start-execution --state-machine-arn <this>"
  value       = aws_sfn_state_machine.transform_chain.arn
}

output "pipeline_alerts_topic_arn" {
  description = "SNS topic the transform chain emails on a blocked gate or a failed step."
  value       = aws_sns_topic.pipeline_alerts.arn
}
