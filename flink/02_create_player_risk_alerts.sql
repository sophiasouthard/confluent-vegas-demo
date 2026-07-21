-- =============================================================================
-- Vegas Gaming Demo — Sub-Task 1 (Step 2 of 3)
-- Sink Table: player_risk_alerts
--
-- Upsert-keyed on (player_id, window_start) — one row per player per minute.
-- Mirrors the fraud_alerts DDL from the fraud demo.
--
-- Run after 01_create_player_events.sql.
-- =============================================================================

CREATE TABLE IF NOT EXISTS player_risk_alerts (
  player_id      STRING,                  -- PRIMARY KEY col FIRST
  window_start   TIMESTAMP(3),            -- PRIMARY KEY col second
  window_end     TIMESTAMP(3),
  bet_count      BIGINT,
  total_wagered  DECIMAL(18, 2),
  avg_bet        DECIMAL(18, 2),
  is_flagged     BOOLEAN,                 -- true if bet_count > 20 OR total_wagered > 10000
  PRIMARY KEY (player_id, window_start) NOT ENFORCED
) WITH (
  'key.format'                     = 'json-registry',
  'value.format'                   = 'json-registry',
  'kafka.consumer.isolation-level' = 'read-uncommitted'
);
