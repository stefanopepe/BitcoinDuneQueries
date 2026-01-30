-- ============================================================
-- Smoke Test: Bitcoin Human Factor Scoring
-- Description: Quick validation query to test the human factor
--              scoring logic on a small sample of recent transactions.
--              Scores transactions on likelihood of human origin.
-- Usage: Copy/paste to Dune and run. Should complete in <30 seconds.
-- ============================================================

WITH
-- Get cutoff block height (last 100 blocks)
block_cutoff AS (
    SELECT MAX(height) - 100 AS min_height FROM bitcoin.blocks
),

-- Raw inputs for block range (non-coinbase only)
-- Note: value is in BTC, spent_block_height enables BDD calculation
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
-- Note: value is in BTC
raw_outputs AS (
    SELECT
        o.tx_id,
        o.value AS output_value_btc
    FROM bitcoin.outputs o
    CROSS JOIN block_cutoff bc
    WHERE o.block_height >= bc.min_height
),

-- Aggregate input features per transaction (including BDD)
tx_input_stats AS (
    SELECT
        tx_id,
        COUNT(*) AS input_count,
        SUM(input_value_btc) AS total_input_btc,
        -- BDD calculation: days_held = (current_block - origin_block) / 144
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
        -- Dust detection: outputs < 546 sats = 0.00000546 BTC
        SUM(CASE WHEN output_value_btc < 0.00000546 THEN 1 ELSE 0 END) AS dust_count,
        -- Round value detection: divisible by 0.001 BTC
        SUM(CASE WHEN output_value_btc > 0 AND ABS(output_value_btc * 1000 - ROUND(output_value_btc * 1000)) < 0.0000001 THEN 1 ELSE 0 END) AS round_value_count
    FROM raw_outputs
    GROUP BY tx_id
),

-- Combine all transaction features
tx_combined AS (
    SELECT
        i.tx_id,
        i.input_count,
        COALESCE(o.output_count, 0) AS output_count,
        i.total_input_btc,
        i.avg_days_held,
        COALESCE(o.dust_count, 0) AS dust_count,
        COALESCE(o.round_value_count, 0) AS round_value_count,
        -- Derived boolean features
        i.input_count > 50 AS is_high_fan_in,
        COALESCE(o.output_count, 0) > 50 AS is_high_fan_out,
        COALESCE(o.dust_count, 0) > 0 AS has_dust,
        COALESCE(o.round_value_count, 0) > 0 AS has_round_values,
        -- Simple structure: 1-in-1-out or 1-in-2-out
        (i.input_count = 1 AND COALESCE(o.output_count, 0) IN (1, 2)) AS is_simple_structure
    FROM tx_input_stats i
    LEFT JOIN tx_output_stats o ON i.tx_id = o.tx_id
),

-- Calculate human factor score per transaction
tx_scored AS (
    SELECT
        tx_id,
        input_count,
        output_count,
        total_input_btc,
        avg_days_held,
        is_high_fan_in,
        is_high_fan_out,
        has_dust,
        has_round_values,
        is_simple_structure,
        -- Calculate raw score
        50  -- BASE_SCORE
        -- NEGATIVE INDICATORS
        + CASE WHEN is_high_fan_in THEN -15 ELSE 0 END
        + CASE WHEN is_high_fan_out THEN -15 ELSE 0 END
        + CASE WHEN has_round_values THEN -5 ELSE 0 END
        + CASE WHEN has_dust THEN -10 ELSE 0 END
        -- POSITIVE INDICATORS
        + CASE WHEN is_simple_structure THEN 10 ELSE 0 END
        + CASE WHEN NOT has_round_values THEN 5 ELSE 0 END
        -- BDD-BASED INDICATORS
        + CASE WHEN avg_days_held >= 1 AND avg_days_held < 365 THEN 10 ELSE 0 END  -- moderate holder
        + CASE WHEN avg_days_held >= 365 THEN 15 ELSE 0 END  -- long-term holder
        AS raw_score
    FROM tx_combined
),

-- Clamp scores and assign to bands
tx_with_bands AS (
    SELECT
        tx_id,
        input_count,
        output_count,
        total_input_btc AS btc_volume,
        avg_days_held,
        GREATEST(0, LEAST(100, raw_score)) AS human_factor_score,
        CASE
            WHEN GREATEST(0, LEAST(100, raw_score)) < 10 THEN '0-10'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 20 THEN '10-20'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 30 THEN '20-30'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 40 THEN '30-40'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 50 THEN '40-50'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 60 THEN '50-60'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 70 THEN '60-70'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 80 THEN '70-80'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 90 THEN '80-90'
            ELSE '90-100'
        END AS score_band,
        CASE
            WHEN GREATEST(0, LEAST(100, raw_score)) < 10 THEN 1
            WHEN GREATEST(0, LEAST(100, raw_score)) < 20 THEN 2
            WHEN GREATEST(0, LEAST(100, raw_score)) < 30 THEN 3
            WHEN GREATEST(0, LEAST(100, raw_score)) < 40 THEN 4
            WHEN GREATEST(0, LEAST(100, raw_score)) < 50 THEN 5
            WHEN GREATEST(0, LEAST(100, raw_score)) < 60 THEN 6
            WHEN GREATEST(0, LEAST(100, raw_score)) < 70 THEN 7
            WHEN GREATEST(0, LEAST(100, raw_score)) < 80 THEN 8
            WHEN GREATEST(0, LEAST(100, raw_score)) < 90 THEN 9
            ELSE 10
        END AS score_band_order
    FROM tx_scored
)

-- Final aggregation by score band (no day grouping for smoke test)
SELECT
    score_band,
    score_band_order,
    COUNT(*) AS tx_count,
    SUM(btc_volume) AS btc_volume,
    AVG(human_factor_score) AS avg_score,
    MIN(human_factor_score) AS min_score,
    MAX(human_factor_score) AS max_score,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM tx_with_bands
GROUP BY score_band, score_band_order
ORDER BY score_band_order;
