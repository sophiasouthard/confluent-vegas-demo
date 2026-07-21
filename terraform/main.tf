# ─────────────────────────────────────────────────────────────────────────────
# Data Sources
# ─────────────────────────────────────────────────────────────────────────────

data "confluent_organization" "main" {}

data "confluent_schema_registry_cluster" "main" {
  environment {
    id = confluent_environment.main.id
  }

  depends_on = [confluent_kafka_cluster.main]
}

data "confluent_flink_region" "main" {
  cloud  = var.cloud_provider
  region = var.region
}

# ─────────────────────────────────────────────────────────────────────────────
# Environment & Cluster
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_environment" "main" {
  display_name = var.environment_name

  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "main" {
  display_name = var.cluster_name
  availability = "SINGLE_ZONE"
  cloud        = var.cloud_provider
  region       = var.region

  basic {}

  environment {
    id = confluent_environment.main.id
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Service Account
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_service_account" "app" {
  display_name = "vegas-gaming-sa"
  description  = "Service account for the Vegas gaming demo pipeline"
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink Compute Pool
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_compute_pool" "main" {
  display_name = "vegas-gaming-pool"
  cloud        = var.cloud_provider
  region       = var.region
  max_cfu      = var.flink_max_cfu

  environment {
    id = confluent_environment.main.id
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# API Keys
# ─────────────────────────────────────────────────────────────────────────────

# Kafka producer API key → scoped to Kafka cluster
resource "confluent_api_key" "kafka_producer" {
  display_name = "vegas-kafka-key"
  description  = "Kafka API key for the Vegas gaming producer"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [confluent_role_binding.kafka_admin]
}

# Schema Registry API key → scoped to Schema Registry cluster
resource "confluent_api_key" "schema_registry" {
  display_name = "vegas-sr-key"
  description  = "Schema Registry API key for the Vegas gaming pipeline"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.main.id
    api_version = data.confluent_schema_registry_cluster.main.api_version
    kind        = data.confluent_schema_registry_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [confluent_role_binding.env_admin]
}

# Flink API key → scoped to Flink region (NOT the Kafka cluster)
resource "confluent_api_key" "flink" {
  display_name = "vegas-flink-key"
  description  = "Flink API key for the Vegas gaming pipeline"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.main.id
    api_version = data.confluent_flink_region.main.api_version
    kind        = data.confluent_flink_region.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [confluent_role_binding.flink_developer]
}

# ─────────────────────────────────────────────────────────────────────────────
# Role Bindings
# ─────────────────────────────────────────────────────────────────────────────

# CloudClusterAdmin → scoped to Kafka cluster (rbac_crn)
resource "confluent_role_binding" "kafka_admin" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main.rbac_crn
}

# FlinkDeveloper → scoped to environment (resource_name)
resource "confluent_role_binding" "flink_developer" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = confluent_environment.main.resource_name
}

# EnvironmentAdmin → scoped to environment (resource_name)
resource "confluent_role_binding" "env_admin" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.main.resource_name
}

# ─────────────────────────────────────────────────────────────────────────────
# RBAC Propagation Delay
# Allow 30 s for role bindings to propagate before Flink statements are submitted
# ─────────────────────────────────────────────────────────────────────────────

resource "time_sleep" "wait_for_rbac" {
  create_duration = "30s"

  depends_on = [
    confluent_role_binding.kafka_admin,
    confluent_role_binding.flink_developer,
    confluent_role_binding.env_admin,
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink SQL — Source Table: player_events
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "create_player_events" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.main.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app.id
  }

  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS player_events (
      player_id    STRING,
      session_id   STRING,
      bet_id       STRING,
      amount       DECIMAL(18, 2),
      game_type    STRING,
      channel      STRING,
      device_id    STRING,
      event_time   TIMESTAMP(3),
      WATERMARK FOR event_time AS event_time - INTERVAL '10' SECONDS
    ) DISTRIBUTED BY (player_id) INTO 4 BUCKETS
    WITH (
      'key.format'                     = 'json-registry',
      'value.format'                   = 'json-registry',
      'kafka.consumer.isolation-level' = 'read-uncommitted'
    );
  SQL

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    time_sleep.wait_for_rbac,
    confluent_api_key.flink,
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink SQL — Sink Table: player_risk_alerts
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "create_player_risk_alerts" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.main.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app.id
  }

  statement = <<-SQL
    CREATE TABLE IF NOT EXISTS player_risk_alerts (
      player_id      STRING,
      window_start   TIMESTAMP(3),
      window_end     TIMESTAMP(3),
      bet_count      BIGINT,
      total_wagered  DECIMAL(18, 2),
      avg_bet        DECIMAL(18, 2),
      is_flagged     BOOLEAN,
      PRIMARY KEY (player_id, window_start) NOT ENFORCED
    ) WITH (
      'key.format'                     = 'json-registry',
      'value.format'                   = 'json-registry',
      'kafka.consumer.isolation-level' = 'read-uncommitted'
    );
  SQL

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    confluent_flink_statement.create_player_events,
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# Flink SQL — Aggregation Job: 1-minute windowed player risk detection
# Flags any player with more than 20 bets OR over $10,000 wagered in a window.
# ─────────────────────────────────────────────────────────────────────────────

resource "confluent_flink_statement" "player_risk_detection_job" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.main.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app.id
  }

  statement = <<-SQL
    INSERT INTO player_risk_alerts
    SELECT
      player_id,
      window_start,
      window_end,
      COUNT(*)                                        AS bet_count,
      SUM(amount)                                     AS total_wagered,
      SUM(amount) / CAST(COUNT(*) AS DECIMAL(18, 2)) AS avg_bet,
      COUNT(*) > 20 OR SUM(amount) > 10000           AS is_flagged
    FROM TABLE(
      TUMBLE(
        TABLE player_events,
        DESCRIPTOR(event_time),
        INTERVAL '1' MINUTE
      )
    )
    GROUP BY player_id, window_start, window_end;
  SQL

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  rest_endpoint = data.confluent_flink_region.main.rest_endpoint

  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [
    confluent_flink_statement.create_player_risk_alerts,
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# Auto-generate python/.env for the producer
# ─────────────────────────────────────────────────────────────────────────────

resource "local_file" "env_file" {
  filename = "${path.module}/../python/.env"
  content  = <<-EOT
KAFKA_BOOTSTRAP_SERVERS=${replace(confluent_kafka_cluster.main.bootstrap_endpoint, "SASL_SSL://", "")}
KAFKA_API_KEY=${confluent_api_key.kafka_producer.id}
KAFKA_API_SECRET=${confluent_api_key.kafka_producer.secret}
SCHEMA_REGISTRY_URL=${data.confluent_schema_registry_cluster.main.rest_endpoint}
SCHEMA_REGISTRY_API_KEY=${confluent_api_key.schema_registry.id}
SCHEMA_REGISTRY_API_SECRET=${confluent_api_key.schema_registry.secret}
EOT
}
