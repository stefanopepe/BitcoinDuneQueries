-- ============================================================
-- Query: Bitcoin Human Factor Scoring V2 (Nested Query)
-- Description: Aggregates human factor scores by day and score band.
--              References bitcoin_tx_features_daily via nested query.
--              Produces same output schema as original query for
--              backward compatibility.
-- Author: stefanopepe
-- Created: 2026-02-02
-- Updated: 2026-02-02
-- Architecture: 1-level nested query on bitcoin_tx_features_daily
-- Base Query: query_<BASE_QUERY_ID> (bitcoin_tx_features_daily)
-- ============================================================
-- Scoring Model (computed in base query):
--   BASE_SCORE = 50 (neutral)
--   Negative (automated signals):
--     high_fan_in (>50 inputs): -15
--     high_fan_out (>50 outputs): -15
--     round_values (divisible by 0.001 BTC): -5
--     dust_output (<546 sats): -10
--   Positive (human signals):
--     simple_structure (1-in-1-out or 1-in-2-out): +10
--     non_round_value: +5
--     moderate_holder (1-365 days): +10
--     long_term_holder (>365 days): +15
--   Final score clamped to [0, 100]
-- ============================================================
-- Score Band Interpretation:
--   0-30:  Likely automated (exchange, pool, bot)
--   30-50: Probably automated
--   50-60: Ambiguous / uncertain
--   60-80: Likely human-controlled
--   80-100: Strong human indicators (HODLer)
-- ============================================================
-- Output Columns:
--   day             - Date of transactions
--   score_band      - Score range (e.g., '50-60')
--   score_band_order- Numeric ordering (1-10)
--   tx_count        - Number of transactions
--   btc_volume      - Total BTC moved (input value)
--   avg_score       - Average exact score in band
-- ============================================================

SELECT
    day,
    score_band,
    score_band_order,
    COUNT(*) AS tx_count,
    SUM(total_input_btc) AS btc_volume,
    AVG(human_factor_score) AS avg_score
FROM query_<BASE_QUERY_ID>
GROUP BY day, score_band, score_band_order
ORDER BY day, score_band_order
