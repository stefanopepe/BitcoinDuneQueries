-- ============================================================
-- Query: Bitcoin Privacy Heuristics V3 (Nested Query)
-- Description: Analyzes privacy issues in transactions classified as
--              "other" by the UTXO intent classification.
--              References bitcoin_tx_features_daily via nested query.
--              Simplified version - uses flags computed in base query.
-- Author: stefanopepe
-- Created: 2026-02-02
-- Updated: 2026-02-02
-- Architecture: 1-level nested query on bitcoin_tx_features_daily
-- Base Query: query_<BASE_QUERY_ID> (bitcoin_tx_features_daily)
-- ============================================================
-- Privacy Heuristics Detected:
--   address_reuse       - Output address matches an input address
--   output_type_mismatch- 2-output tx with different script types
--   no_issue_detected   - No privacy issues detected
-- Note: Advanced heuristics (change_precision, UIH1, UIH2) require
--       detailed output analysis not available in base query.
--       Use bitcoin_privacy_heuristics_v2.sql for full analysis.
-- ============================================================
-- Output Columns:
--   day                - Date of transactions
--   privacy_heuristic  - The privacy issue detected
--   tx_count           - Number of transactions
--   sats_total         - Total BTC involved
-- ============================================================

SELECT
    day,
    CASE
        WHEN has_address_reuse THEN 'address_reuse'
        WHEN output_type_mismatch THEN 'output_type_mismatch'
        ELSE 'no_issue_detected'
    END AS privacy_heuristic,
    COUNT(*) AS tx_count,
    SUM(total_input_btc) AS sats_total
FROM query_<BASE_QUERY_ID>
WHERE intent = 'other'
GROUP BY day,
    CASE
        WHEN has_address_reuse THEN 'address_reuse'
        WHEN output_type_mismatch THEN 'output_type_mismatch'
        ELSE 'no_issue_detected'
    END
ORDER BY day, privacy_heuristic
