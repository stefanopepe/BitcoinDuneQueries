-- ============================================================
-- Query: Bitcoin Human Factor + Cohort Matrix (Nested Query)
-- Description: Cross-tabulates human factor score bands with BTC
--              volume cohorts (Shrimps through Humpback).
--              References bitcoin_tx_features_daily via nested query.
--              Enables analysis of how scoring varies by holder size.
-- Author: stefanopepe
-- Created: 2026-02-02
-- Updated: 2026-02-02
-- Architecture: 1-level nested query on bitcoin_tx_features_daily
-- Base Query: query_6638509 (https://dune.com/queries/6638509)
-- ============================================================
-- Matrix Dimensions:
--   Rows (Score Bands): 10 bands from 0-10 to 90-100
--   Columns (Cohorts): 8 cohorts from Shrimps to Humpback
--   Result: Up to 80 cells per day (10 × 8)
-- ============================================================
-- Cohort Definitions (by total tx input value in BTC):
--   Shrimps    - < 1 BTC
--   Crab       - 1-10 BTC
--   Octopus    - 10-50 BTC
--   Fish       - 50-100 BTC
--   Dolphin    - 100-500 BTC
--   Shark      - 500-1,000 BTC
--   Whale      - 1,000-5,000 BTC
--   Humpback   - > 5,000 BTC
-- ============================================================
-- Parameters:
--   {{start_date}} - Analysis start date (default: 30 days ago)
--   {{end_date}}   - Analysis end date (default: today)
-- ============================================================
-- Output Columns:
--   day              - Date of transactions
--   score_band       - Score range (e.g., '50-60')
--   score_band_order - Numeric ordering (1-10)
--   cohort           - Holder cohort name
--   cohort_order     - Numeric ordering (1-8)
--   tx_count         - Number of transactions
--   btc_volume       - Total BTC moved
--   avg_score        - Average score in segment
-- ============================================================

SELECT
    day,
    score_band,
    score_band_order,
    cohort,
    cohort_order,
    COUNT(*) AS tx_count,
    SUM(total_input_btc) AS btc_volume,
    AVG(human_factor_score) AS avg_score
FROM query_6638509
WHERE day >= DATE '{{start_date}}'
  AND day < DATE '{{end_date}}'
GROUP BY day, score_band, score_band_order, cohort, cohort_order
ORDER BY day, score_band_order, cohort_order
