-- Canonical Base lending intent time series for dashboard visualization.
-- Window defaults are controlled by caller placeholders.
WITH
loop_metrics AS (
  SELECT
    day,
    total_borrow_volume_usd,
    looped_borrow_volume_usd,
    loop_share_by_volume,
    unique_loop_entities
  FROM query_<BASE_LENDING_LOOP_METRICS_DAILY_ID>
  WHERE day >= CAST(TIMESTAMP '{start_ts}' AS DATE)
    AND day < CAST(TIMESTAMP '{end_ts}' AS DATE)
),
flow_pairs AS (
  SELECT
    block_date AS day,
    COUNT(DISTINCT CAST(source_protocol AS VARCHAR) || '->' || CAST(dest_protocol AS VARCHAR)) AS active_protocol_pairs,
    COUNT(*) AS flow_count
  FROM query_<BASE_LENDING_FLOW_STITCHING_ID>
  WHERE block_date >= CAST(TIMESTAMP '{start_ts}' AS DATE)
    AND block_date < CAST(TIMESTAMP '{end_ts}' AS DATE)
  GROUP BY 1
)
SELECT
  lm.day,
  lm.total_borrow_volume_usd,
  lm.looped_borrow_volume_usd,
  lm.loop_share_by_volume,
  lm.unique_loop_entities,
  COALESCE(fp.active_protocol_pairs, 0) AS active_protocol_pairs,
  COALESCE(fp.flow_count, 0) AS flow_count
FROM loop_metrics lm
LEFT JOIN flow_pairs fp
  ON fp.day = lm.day
ORDER BY lm.day
