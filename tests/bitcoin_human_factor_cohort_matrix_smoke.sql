-- ============================================================
-- Smoke Test: Bitcoin Human Factor + Cohort Matrix
-- Description: Quick validation query to test the cross-tabulation
--              of human factor score bands with BTC volume cohorts.
--              Validates matrix dimensions and aggregation logic.
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
        i.block_height,
        i.spent_block_height,
        i.value AS input_value_btc
    FROM bitcoin.inputs i
    CROSS JOIN block_cutoff bc
    WHERE i.block_height >= bc.min_height
      AND i.is_coinbase = FALSE
),

-- Raw outputs for block range
raw_outputs AS (
    SELECT
        o.tx_id,
        o.value AS output_value_btc
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
        ) AS avg_days_held
    FROM raw_inputs
    GROUP BY tx_id
),

-- Aggregate output features per transaction
tx_output_stats AS (
    SELECT
        tx_id,
        COUNT(*) AS output_count,
        SUM(CASE WHEN output_value_btc < 0.00000546 THEN 1 ELSE 0 END) AS dust_output_count,
        SUM(CASE WHEN output_value_btc > 0 AND ABS(output_value_btc * 1000 - ROUND(output_value_btc * 1000)) < 0.0000001 THEN 1 ELSE 0 END) AS round_value_count
    FROM raw_outputs
    GROUP BY tx_id
),

-- Combine and score
tx_scored AS (
    SELECT
        i.tx_id,
        i.input_count,
        COALESCE(o.output_count, 0) AS output_count,
        i.total_input_btc,
        i.avg_days_held,
        -- Human factor score
        GREATEST(0, LEAST(100,
            50
            + CASE WHEN i.input_count > 50 THEN -15 ELSE 0 END
            + CASE WHEN COALESCE(o.output_count, 0) > 50 THEN -15 ELSE 0 END
            + CASE WHEN COALESCE(o.round_value_count, 0) > 0 THEN -5 ELSE 0 END
            + CASE WHEN COALESCE(o.dust_output_count, 0) > 0 THEN -10 ELSE 0 END
            + CASE WHEN (i.input_count = 1 AND COALESCE(o.output_count, 0) IN (1, 2)) THEN 10 ELSE 0 END
            + CASE WHEN COALESCE(o.round_value_count, 0) = 0 THEN 5 ELSE 0 END
            + CASE WHEN i.avg_days_held >= 1 AND i.avg_days_held < 365 THEN 10 ELSE 0 END
            + CASE WHEN i.avg_days_held >= 365 THEN 15 ELSE 0 END
        )) AS human_factor_score
    FROM tx_input_stats i
    LEFT JOIN tx_output_stats o ON i.tx_id = o.tx_id
),

-- Add bands and cohorts
tx_final AS (
    SELECT
        tx_id,
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
        CASE
            WHEN human_factor_score < 10 THEN 1
            WHEN human_factor_score < 20 THEN 2
            WHEN human_factor_score < 30 THEN 3
            WHEN human_factor_score < 40 THEN 4
            WHEN human_factor_score < 50 THEN 5
            WHEN human_factor_score < 60 THEN 6
            WHEN human_factor_score < 70 THEN 7
            WHEN human_factor_score < 80 THEN 8
            WHEN human_factor_score < 90 THEN 9
            ELSE 10
        END AS score_band_order,
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
        CASE
            WHEN total_input_btc < 1 THEN 1
            WHEN total_input_btc < 10 THEN 2
            WHEN total_input_btc < 50 THEN 3
            WHEN total_input_btc < 100 THEN 4
            WHEN total_input_btc < 500 THEN 5
            WHEN total_input_btc < 1000 THEN 6
            WHEN total_input_btc < 5000 THEN 7
            ELSE 8
        END AS cohort_order
    FROM tx_scored
)

-- ============================================================
-- VALIDATION QUERIES - Uncomment one at a time to test
-- ============================================================

-- 1. Cross-tabulation Matrix (main output)
SELECT
    score_band,
    score_band_order,
    cohort,
    cohort_order,
    COUNT(*) AS tx_count,
    ROUND(SUM(total_input_btc), 4) AS btc_volume,
    ROUND(AVG(human_factor_score), 1) AS avg_score
FROM tx_final
GROUP BY score_band, score_band_order, cohort, cohort_order
ORDER BY score_band_order, cohort_order

-- 2. Matrix Dimensions Check (uncomment to verify 10 bands × 8 cohorts max)
-- SELECT
--     COUNT(DISTINCT score_band) AS distinct_bands,
--     COUNT(DISTINCT cohort) AS distinct_cohorts,
--     COUNT(*) AS total_cells
-- FROM (
--     SELECT score_band, cohort FROM tx_final GROUP BY score_band, cohort
-- );

-- 3. Score Band Totals (should match human_factor_scoring_v2)
-- SELECT score_band, COUNT(*) AS tx_count, ROUND(SUM(total_input_btc), 4) AS btc_volume
-- FROM tx_final GROUP BY score_band ORDER BY score_band;

-- 4. Cohort Totals (should match cohort_distribution_v2)
-- SELECT cohort, COUNT(*) AS tx_count, ROUND(SUM(total_input_btc), 4) AS btc_moved
-- FROM tx_final GROUP BY cohort ORDER BY cohort;

-- 5. Sample Transactions
-- SELECT * FROM tx_final LIMIT 10;
