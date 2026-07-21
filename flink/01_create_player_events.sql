-- =============================================================================
-- Vegas Gaming Demo — Step 1 of 3
-- Source Table: player_events
--
-- Run in the Confluent Cloud Flink SQL workspace:
--   sql.current-catalog  = <your environment display name>
--   sql.current-database = <your cluster display name>
-- =============================================================================

CREATE TABLE IF NOT EXISTS player_events (
  player_id    STRING,                   -- distribution / key column FIRST
  session_id   STRING,
  bet_id       STRING,
  amount       DECIMAL(18, 2),
  game_type    STRING,                   -- BLACKJACK | SLOTS | ROULETTE | POKER | SPORTS_BOOK
  channel      STRING,                   -- FLOOR | ONLINE | MOBILE | KIOSK
  device_id    STRING,
  event_time   TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECONDS
) DISTRIBUTED BY (player_id) INTO 4 BUCKETS
WITH (
  'key.format'                     = 'json-registry',
  'value.format'                   = 'json-registry',
  'kafka.consumer.isolation-level' = 'read-uncommitted'
);
