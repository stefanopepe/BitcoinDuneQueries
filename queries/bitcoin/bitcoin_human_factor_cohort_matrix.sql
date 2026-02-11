-- ============================================================
-- Query: Bitcoin Human Factor + Cohort Matrix (Nested Query)
-- Description: Cross-tabulates human factor score bands with BTC
--              volume cohorts (Shrimps through Humpback).
--              References bitcoin_tx_features_daily via nested query.
--              Enables analysis of how scoring varies by holder size.
-- Author: stefanopepe
-- Created: 2026-02-02
-- Updated: 2026-02-10
-- Architecture: 1-level nested query on bitcoin_tx_features_daily
-- Base Query: query_6638509 (https://dune.com/queries/6638509)
-- ============================================================
-- IMPORTANT: Result Set Characteristics
-- - The matrix is SPARSE: many (day, score_band, cohort) cells
--   have zero transactions and are ABSENT from results
-- - Missing cells are not present with zero values - they're not
--   in the result set at all
-- - Dashboard visualizations MUST apply zero-fill densification
-- - See docs/dashboard_brief_human_factor_cohort_matrix.md Section 5.0
--   for zero-fill SQL patterns
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
--   {{start_date}} - Analysis start date (Dune date picker)
--   {{end_date}}   - Analysis end date (Dune date picker)
--
-- Parameter Configuration Notes:
--   - Dune parameters require UI configuration (date picker widgets)
--   - No SQL-level defaults supported by Dune platform
--   - Base query (6638509) has fallback DATE '2026-01-01'
--   - Configure dashboard date picker minimum: 2026-01-01
--   - See dashboard brief Section 6 for full parameter details
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
--   avg_fee_btc      - Average transaction fee in BTC
--   total_fee_btc    - Total fees paid in BTC
--   tx_with_address_reuse - Count of txs with address reuse
--   tx_with_output_mismatch - Count of txs with output type mismatch
--   pct_address_reuse - Percentage of txs with address reuse
-- ============================================================

SELECT
    day,
    score_band,
    score_band_order,
    cohort,
    cohort_order,
    COUNT(*) AS tx_count,
    SUM(total_input_btc) AS btc_volume,
    AVG(human_factor_score) AS avg_score,
    -- Fee analysis metrics
    AVG(fee_btc) AS avg_fee_btc,
    SUM(fee_btc) AS total_fee_btc,
    -- Privacy metrics
    SUM(CASE WHEN has_address_reuse THEN 1 ELSE 0 END) AS tx_with_address_reuse,
    SUM(CASE WHEN output_type_mismatch THEN 1 ELSE 0 END) AS tx_with_output_mismatch,
    ROUND(100.0 * SUM(CASE WHEN has_address_reuse THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_address_reuse
FROM query_6638509
WHERE day >= CAST('{{start_date}}' AS TIMESTAMP)
  AND day < CAST('{{end_date}}' AS TIMESTAMP)
GROUP BY day, score_band, score_band_order, cohort, cohort_order
ORDER BY day, score_band_order, cohort_order
