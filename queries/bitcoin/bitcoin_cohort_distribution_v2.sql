-- ============================================================
-- Query: Bitcoin Cohort Distribution V2 (Nested Query)
-- Description: Aggregates transactions by day and BTC volume cohort.
--              References bitcoin_tx_features_daily via nested query.
--              Produces same output schema as original query for
--              backward compatibility.
-- Author: stefanopepe
-- Created: 2026-02-02
-- Updated: 2026-02-02
-- Architecture: 1-level nested query on bitcoin_tx_features_daily
-- Base Query: query_<BASE_QUERY_ID> (bitcoin_tx_features_daily)
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
-- Output Columns:
--   day              - Date of transactions
--   cohort           - Holder cohort name
--   cohort_order     - Numeric ordering for cohorts (1-8)
--   btc_moved        - Total BTC moved (input value)
--   tx_count         - Number of transactions
--   spent_utxo_count - Total UTXOs consumed (input count)
-- ============================================================

SELECT
    day,
    cohort,
    cohort_order,
    SUM(total_input_btc) AS btc_moved,
    COUNT(*) AS tx_count,
    SUM(input_count) AS spent_utxo_count
FROM query_<BASE_QUERY_ID>
GROUP BY day, cohort, cohort_order
ORDER BY day, cohort_order
