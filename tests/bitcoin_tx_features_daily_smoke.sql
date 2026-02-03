-- ============================================================
-- Smoke Test: Bitcoin Transaction Features Daily (Base Query)
-- Description: Quick validation query to test the unified base query
--              logic on a small sample of recent transactions.
--              Validates all computed fields: scores, bands, cohorts, intents.
-- Usage: Copy/paste to Dune and run. Should complete in <30 seconds.
-- ============================================================

WITH
-- Get cutoff block height (last 100 blocks ≈ ~16 hours)
block_cutoff AS (
    SELECT MAX(height) - 100 AS min_height FROM bitcoin.blocks
),

-- Raw inputs for block range (non-coinbase only)
raw_inputs AS (
    SELECT
        i.tx_id,
        i.index AS input_index,
        i.block_height,
        i.spent_block_height,
        i.value AS input_value_btc,
        i.address AS input_address,
        i.type AS input_script_type
    FROM bitcoin.inputs i
    CROSS JOIN block_cutoff bc
    WHERE i.block_height >= bc.min_height
      AND i.is_coinbase = FALSE
),

-- Raw outputs for block range
raw_outputs AS (
    SELECT
        o.tx_id,
        o.index AS output_index,
        o.value AS output_value_btc,
        o.address AS output_address,
        o.type AS output_script_type
    FROM bitcoin.outputs o
    CROSS JOIN block_cutoff bc
    WHERE o.block_height >= bc.min_height
),

-- Aggregate input features per transaction
tx_input_stats AS (
    SELECT
        tx_id,
        COUNT(*) AS input_count,
        SUM(input_value_btc) AS total_input_btc,
        AVG(
            CASE
                WHEN spent_block_height IS NOT NULL
                THEN (block_height - spent_block_height) / 144.0
                ELSE NULL
            END
        ) AS avg_days_held,
        ARRAY_AGG(DISTINCT input_address) FILTER (WHERE input_address IS NOT NULL) AS input_addresses
    FROM raw_inputs
    GROUP BY tx_id
),

-- Aggregate output features per transaction
tx_output_stats AS (
    SELECT
        tx_id,
        COUNT(*) AS output_count,
        SUM(output_value_btc) AS total_output_btc,
        SUM(CASE WHEN output_value_btc < 0.00000546 THEN 1 ELSE 0 END) AS dust_output_count,
        SUM(CASE WHEN output_value_btc > 0 AND ABS(output_value_btc * 1000 - ROUND(output_value_btc * 1000)) < 0.0000001 THEN 1 ELSE 0 END) AS round_value_count,
        COUNT(DISTINCT output_script_type) AS distinct_output_script_count
    FROM raw_outputs
    GROUP BY tx_id
),

-- Detect address reuse
address_reuse_detection AS (
    SELECT DISTINCT
        ri.tx_id,
        TRUE AS has_address_reuse
    FROM raw_inputs ri
    INNER JOIN raw_outputs ro
        ON ri.tx_id = ro.tx_id
        AND ri.input_address = ro.output_address
        AND ri.input_address IS NOT NULL
),

-- Combine all transaction features
tx_combined AS (
    SELECT
        i.tx_id,
        i.input_count,
        COALESCE(o.output_count, 0) AS output_count,
        i.total_input_btc,
        COALESCE(o.total_output_btc, 0) AS total_output_btc,
        i.total_input_btc - COALESCE(o.total_output_btc, 0) AS fee_btc,
        COALESCE(o.dust_output_count, 0) AS dust_output_count,
        COALESCE(o.round_value_count, 0) AS round_value_count,
        i.avg_days_held,
        COALESCE(ar.has_address_reuse, FALSE) AS has_address_reuse,
        (COALESCE(o.output_count, 0) = 2 AND COALESCE(o.distinct_output_script_count, 0) > 1) AS output_type_mismatch,
        i.input_count > 50 AS is_high_fan_in,
        COALESCE(o.output_count, 0) > 50 AS is_high_fan_out,
        COALESCE(o.dust_output_count, 0) > 0 AS has_dust,
        COALESCE(o.round_value_count, 0) > 0 AS has_round_values,
        (i.input_count = 1 AND COALESCE(o.output_count, 0) IN (1, 2)) AS is_simple_structure
    FROM tx_input_stats i
    LEFT JOIN tx_output_stats o ON i.tx_id = o.tx_id
    LEFT JOIN address_reuse_detection ar ON i.tx_id = ar.tx_id
),

-- Calculate score and classify
tx_scored AS (
    SELECT
        tx_id,
        input_count,
        output_count,
        total_input_btc,
        total_output_btc,
        fee_btc,
        dust_output_count,
        round_value_count,
        avg_days_held,
        has_address_reuse,
        output_type_mismatch,
        -- Human factor score
        GREATEST(0, LEAST(100,
            50
            + CASE WHEN is_high_fan_in THEN -15 ELSE 0 END
            + CASE WHEN is_high_fan_out THEN -15 ELSE 0 END
            + CASE WHEN has_round_values THEN -5 ELSE 0 END
            + CASE WHEN has_dust THEN -10 ELSE 0 END
            + CASE WHEN is_simple_structure THEN 10 ELSE 0 END
            + CASE WHEN NOT has_round_values THEN 5 ELSE 0 END
            + CASE WHEN avg_days_held >= 1 AND avg_days_held < 365 THEN 10 ELSE 0 END
            + CASE WHEN avg_days_held >= 365 THEN 15 ELSE 0 END
        )) AS human_factor_score,
        -- Intent classification
        CASE
            WHEN output_count = 0 THEN 'malformed_no_outputs'
            WHEN input_count >= 10 AND output_count <= 2 THEN 'consolidation'
            WHEN input_count <= 2 AND output_count >= 10 THEN 'fan_out_batch'
            WHEN input_count >= 5 AND output_count >= 5 AND ABS(input_count - output_count) <= 1 THEN 'coinjoin_like'
            WHEN input_count = 1 AND output_count = 1 THEN 'self_transfer'
            WHEN output_count = 2 AND input_count >= 2 THEN 'change_like_2_outputs'
            ELSE 'other'
        END AS intent
    FROM tx_combined
),

-- Add bands and cohorts
tx_final AS (
    SELECT
        tx_id,
        input_count,
        output_count,
        total_input_btc,
        human_factor_score,
        -- Score band
        CASE
            WHEN human_factor_score < 10 THEN '0-10'
            WHEN human_factor_score < 20 THEN '10-20'
            WHEN human_factor_score < 30 THEN '20-30'
            WHEN human_factor_score < 40 THEN '30-40'
            WHEN human_factor_score < 50 THEN '40-50'
            WHEN human_factor_score < 60 THEN '50-60'
            WHEN human_factor_score < 70 THEN '60-70'
            WHEN human_factor_score < 80 THEN '70-80'
            WHEN human_factor_score < 90 THEN '80-90'
            ELSE '90-100'
        END AS score_band,
        -- Cohort
        CASE
            WHEN total_input_btc < 1 THEN 'Shrimps (<1 BTC)'
            WHEN total_input_btc < 10 THEN 'Crab (1-10 BTC)'
            WHEN total_input_btc < 50 THEN 'Octopus (10-50 BTC)'
            WHEN total_input_btc < 100 THEN 'Fish (50-100 BTC)'
            WHEN total_input_btc < 500 THEN 'Dolphin (100-500 BTC)'
            WHEN total_input_btc < 1000 THEN 'Shark (500-1,000 BTC)'
            WHEN total_input_btc < 5000 THEN 'Whale (1,000-5,000 BTC)'
            ELSE 'Humpback (>5,000 BTC)'
        END AS cohort,
        intent,
        has_address_reuse,
        output_type_mismatch
    FROM tx_scored
)

-- ============================================================
-- VALIDATION QUERIES - Uncomment one at a time to test
-- ============================================================

-- 1. Score Distribution (should have values in multiple bands)
SELECT
    score_band,
    COUNT(*) AS tx_count,
    ROUND(AVG(human_factor_score), 1) AS avg_score,
    ROUND(MIN(total_input_btc), 4) AS min_btc,
    ROUND(MAX(total_input_btc), 4) AS max_btc
FROM tx_final
GROUP BY score_band
ORDER BY score_band

-- 2. Cohort Distribution (uncomment to test)
-- SELECT cohort, COUNT(*) AS tx_count, ROUND(SUM(total_input_btc), 2) AS total_btc
-- FROM tx_final GROUP BY cohort ORDER BY cohort;

-- 3. Intent Distribution (uncomment to test)
-- SELECT intent, COUNT(*) AS tx_count FROM tx_final GROUP BY intent ORDER BY intent;

-- 4. Cross-tabulation Matrix Sample (uncomment to test)
-- SELECT score_band, cohort, COUNT(*) AS tx_count
-- FROM tx_final GROUP BY score_band, cohort ORDER BY score_band, cohort;

-- 5. Privacy Flags (uncomment to test)
-- SELECT has_address_reuse, output_type_mismatch, COUNT(*) AS tx_count
-- FROM tx_final WHERE intent = 'other' GROUP BY 1, 2;

-- 6. Sample Transactions with All Fields (uncomment to test)
-- SELECT * FROM tx_final LIMIT 20;
