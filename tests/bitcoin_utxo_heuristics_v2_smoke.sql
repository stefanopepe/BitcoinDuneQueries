-- ============================================================
-- Smoke Test: Bitcoin UTXO Heuristics V2
-- Description: Validates that intent classification aggregation
--              produces expected categories with non-zero counts.
--              Uses bitcoin.inputs and bitcoin.outputs for last 7 days.
-- Author: stefanopepe
-- Created: 2026-02-17
-- Updated: 2026-02-17
-- ============================================================

WITH raw_inputs AS (
    SELECT
        CAST(date_trunc('day', block_time) AS DATE) AS day,
        tx_id,
        COUNT(*) AS input_count,
        SUM(value) AS total_input_btc
    FROM bitcoin.inputs
    WHERE block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND block_time < CURRENT_DATE
      AND NOT is_coinbase
    GROUP BY 1, 2
),

raw_outputs AS (
    SELECT
        tx_id,
        COUNT(*) AS output_count,
        SUM(value) AS total_output_btc
    FROM bitcoin.outputs
    WHERE block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND block_time < CURRENT_DATE
    GROUP BY 1
),

tx_features AS (
    SELECT
        i.day,
        i.tx_id,
        i.input_count,
        COALESCE(o.output_count, 0) AS output_count,
        i.total_input_btc,
        COALESCE(o.total_output_btc, 0) AS total_output_btc,
        CASE
            WHEN i.input_count >= 10 AND COALESCE(o.output_count, 0) <= 2 THEN 'consolidation'
            WHEN i.input_count <= 2 AND COALESCE(o.output_count, 0) >= 10 THEN 'fan_out_batch'
            WHEN i.input_count >= 5 AND COALESCE(o.output_count, 0) >= 5
                 AND ABS(i.input_count - COALESCE(o.output_count, 0)) <= 1 THEN 'coinjoin_like'
            WHEN i.input_count = 1 AND COALESCE(o.output_count, 0) = 1 THEN 'self_transfer'
            WHEN i.input_count >= 2 AND COALESCE(o.output_count, 0) = 2 THEN 'change_like_2_outputs'
            WHEN COALESCE(o.output_count, 0) = 0 THEN 'malformed_no_outputs'
            ELSE 'other'
        END AS intent
    FROM raw_inputs i
    LEFT JOIN raw_outputs o ON o.tx_id = i.tx_id
)

-- Validate: each intent category should have transactions
SELECT
    'intent_distribution' AS test_name,
    intent,
    COUNT(*) AS tx_count,
    SUM(total_input_btc) AS sats_in,
    SUM(total_output_btc) AS sats_out,
    AVG(input_count) AS avg_inputs,
    AVG(output_count) AS avg_outputs
FROM tx_features
GROUP BY intent
ORDER BY tx_count DESC
