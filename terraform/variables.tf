variable "api_key" {
  description = "Confluent Cloud API key (Cloud-level, not Kafka)"
  type        = string
  sensitive   = true
}

variable "api_secret" {
  description = "Confluent Cloud API secret"
  type        = string
  sensitive   = true
}

variable "environment_name" {
  description = "Name for the Confluent Cloud environment"
  type        = string
  default     = "vegas-gaming-env"
}

variable "cluster_name" {
  description = "Name for the Kafka cluster"
  type        = string
  default     = "vegas-cluster"
}

variable "region" {
  description = "Cloud region for the Kafka cluster and Flink compute pool"
  type        = string
  default     = "us-east-1"
}

variable "cloud_provider" {
  description = "Cloud provider (AWS, GCP, or AZURE)"
  type        = string
  default     = "AWS"

  validation {
    condition     = contains(["AWS", "GCP", "AZURE"], var.cloud_provider)
    error_message = "cloud_provider must be one of: AWS, GCP, AZURE."
  }
}

variable "flink_max_cfu" {
  description = "Maximum Confluent Flink Units (CFUs) for the Flink compute pool"
  type        = number
  default     = 5
}
