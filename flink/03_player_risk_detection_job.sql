-- =============================================================================
-- Vegas Gaming Demo — Sub-Task 1 (Step 3 of 3)
-- Aggregation Job: Real-Time Player Risk Detection
--
-- Uses a 1-minute TUMBLE window on event_time to aggregate per player.
-- Flags players with:
--   • bet_count    > 20        (rapid betting pattern)
--   • total_wagered > 10000    (high-value threshold)
--
-- Mirrors the fraud_detection_job INSERT from the fraud demo,
-- adapted to 1-minute windows and gaming risk thresholds.
--
-- Run after 02_create_player_risk_alerts.sql.
-- This statement is long-running — Flink status should show RUNNING.
-- =============================================================================

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
