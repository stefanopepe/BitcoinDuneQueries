-- ============================================================
-- Query: Bitcoin UTXO Heuristics V2 (Nested Query)
-- Description: Aggregates transactions by day and intent classification.
--              References bitcoin_tx_features_daily via nested query.
--              Produces same output schema as original query for
--              backward compatibility.
-- Author: stefanopepe
-- Created: 2026-02-02
-- Updated: 2026-02-02
-- Architecture: 1-level nested query on bitcoin_tx_features_daily
-- Base Query: query_6638509 (bitcoin_tx_features_daily)
-- ============================================================
-- Intent Classifications (computed in base query):
--   consolidation        - >=10 inputs, <=2 outputs
--   fan_out_batch        - <=2 inputs, >=10 outputs
--   coinjoin_like        - >=5 inputs, >=5 outputs, |diff| <= 1
--   self_transfer        - 1 input, 1 output
--   change_like_2_outputs- >=2 inputs, 2 outputs
--   other                - Everything else
--   malformed_no_outputs - 0 outputs (rare edge case)
-- ============================================================
-- Output Columns:
--   day            - Date of transactions
--   intent         - Classified transaction intent
--   tx_count       - Number of transactions
--   sats_in        - Total input value (BTC, named for compatibility)
--   sats_out       - Total output value (BTC, named for compatibility)
--   avg_inputs     - Average input count per tx
--   avg_outputs    - Average output count per tx
--   median_inputs  - Median input count per tx
--   median_outputs - Median output count per tx
-- ============================================================

SELECT
    day,
    intent,
    COUNT(*) AS tx_count,
    SUM(total_input_btc) AS sats_in,
    SUM(total_output_btc) AS sats_out,
    AVG(input_count) AS avg_inputs,
    AVG(output_count) AS avg_outputs,
    APPROX_PERCENTILE(input_count, 0.5) AS median_inputs,
    APPROX_PERCENTILE(output_count, 0.5) AS median_outputs
FROM query_6638509
GROUP BY day, intent
ORDER BY day, intent
