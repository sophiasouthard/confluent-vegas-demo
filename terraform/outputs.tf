output "environment_id" {
  description = "Confluent Cloud environment ID"
  value       = confluent_environment.main.id
}

output "cluster_id" {
  description = "Kafka cluster ID"
  value       = confluent_kafka_cluster.main.id
}

output "kafka_bootstrap_servers" {
  description = "Kafka bootstrap endpoint (without SASL_SSL:// prefix)"
  value       = replace(confluent_kafka_cluster.main.bootstrap_endpoint, "SASL_SSL://", "")
}

output "kafka_rest_endpoint" {
  description = "Kafka REST endpoint"
  value       = confluent_kafka_cluster.main.rest_endpoint
}

output "kafka_producer_api_key" {
  description = "Kafka producer API key"
  value       = confluent_api_key.kafka_producer.id
  sensitive   = true
}

output "kafka_producer_api_secret" {
  description = "Kafka producer API secret"
  value       = confluent_api_key.kafka_producer.secret
  sensitive   = true
}

output "schema_registry_url" {
  description = "Schema Registry REST endpoint"
  value       = data.confluent_schema_registry_cluster.main.rest_endpoint
}

output "schema_registry_api_key" {
  description = "Schema Registry API key"
  value       = confluent_api_key.schema_registry.id
  sensitive   = true
}

output "schema_registry_api_secret" {
  description = "Schema Registry API secret"
  value       = confluent_api_key.schema_registry.secret
  sensitive   = true
}

output "flink_rest_endpoint" {
  description = "Flink REST endpoint"
  value       = data.confluent_flink_region.main.rest_endpoint
}

output "flink_api_key" {
  description = "Flink API key"
  value       = confluent_api_key.flink.id
  sensitive   = true
}

output "flink_api_secret" {
  description = "Flink API secret"
  value       = confluent_api_key.flink.secret
  sensitive   = true
}
