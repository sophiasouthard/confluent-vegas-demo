-- =============================================================================
-- Vegas Gaming Demo — Sub-Task 1 (Step 2 of 3)
-- Sink Table: player_risk_alerts
--
-- Append-only (no PRIMARY KEY) so the Confluent RTCE queryData tool can read it.
-- Flink writes one row per player per 1-minute window close.
--
-- Run after 01_create_player_events.sql.
-- =============================================================================

CREATE TABLE IF NOT EXISTS player_risk_alerts_v2 (
  player_id      STRING,
  window_start   TIMESTAMP(3),
  window_end     TIMESTAMP(3),
  bet_count      BIGINT,
  total_wagered  DECIMAL(18, 2),
  avg_bet        DECIMAL(18, 2),
  is_flagged     BOOLEAN                  -- true if bet_count > 20 OR total_wagered > 10000
) WITH (
  'value.format'                   = 'json-registry',
  'kafka.consumer.isolation-level' = 'read-uncommitted'
);
