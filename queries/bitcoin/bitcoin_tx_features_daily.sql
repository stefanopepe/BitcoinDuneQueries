-- ============================================================
-- Query: Bitcoin Transaction Features Daily (Base Query)
-- Description: Unified base query that computes ALL transaction-level
--              features for downstream nested queries. Fetches data from
--              bitcoin.inputs and bitcoin.outputs ONCE, then computes:
--              - Core metrics (input/output counts, values, fees)
--              - Human factor scoring (BDD, dust, round values)
--              - Cohort classification (Shrimps through Humpback)
--              - UTXO intent classification (consolidation, fan-out, etc.)
--              - Privacy flags (address reuse, script type mismatch)
--              Uses incremental processing with 1-day lookback.
-- Author: stefanopepe
-- Created: 2026-02-02
-- Updated: 2026-02-02
-- Architecture: This is the BASE QUERY for the unified architecture.
--               All downstream queries reference this via nested queries.
-- Note: On first run, only processes data from fallback date onwards.
--       Adjust DATE '2026-01-01' in checkpoint CTE for historical analysis.
-- ============================================================
-- Downstream Queries (all 1-level deep):
--   - bitcoin_human_factor_scoring_v2.sql (GROUP BY day, score_band)
--   - bitcoin_human_factor_cohort_matrix.sql (GROUP BY day, score_band, cohort)
--   - bitcoin_cohort_distribution_v2.sql (GROUP BY day, cohort)
--   - bitcoin_utxo_heuristics_v2.sql (GROUP BY day, intent)
--   - bitcoin_privacy_heuristics_v3.sql (GROUP BY day, privacy_heuristic)
-- ============================================================
-- Output Columns:
--   Core Fields:
--     day                  - Date of transaction
--     tx_id                - Transaction identifier
--     input_count          - Number of inputs
--     output_count         - Number of outputs
--     total_input_btc      - Total input value (BTC)
--     total_output_btc     - Total output value (BTC)
--     fee_btc              - Transaction fee (BTC)
--   Human Factor Fields:
--     dust_output_count    - Outputs < 546 sats
--     round_value_count    - Outputs divisible by 0.001 BTC
--     avg_days_held        - Average BDD (Bitcoin Days Destroyed)
--     human_factor_score   - Score 0-100 (automated to human)
--     score_band           - Score range (e.g., '50-60')
--     score_band_order     - Numeric ordering 1-10
--   Cohort Fields:
--     cohort               - Volume cohort (Shrimps, Crab, etc.)
--     cohort_order         - Numeric ordering 1-8
--   Classification Fields:
--     intent               - UTXO classification
--     has_address_reuse    - Output addr matches input addr
--     output_type_mismatch - Different output script types (for 2-out tx)
-- ============================================================

WITH
-- 1) Previous results (empty on first ever run)
prev AS (
    SELECT *
    FROM TABLE(previous.query.result(
        schema => DESCRIPTOR(
            day DATE,
            tx_id VARBINARY,
            input_count BIGINT,
            output_count BIGINT,
            total_input_btc DOUBLE,
            total_output_btc DOUBLE,
            fee_btc DOUBLE,
            dust_output_count BIGINT,
            round_value_count BIGINT,
            avg_days_held DOUBLE,
            human_factor_score BIGINT,
            score_band VARCHAR,
            score_band_order BIGINT,
            cohort VARCHAR,
            cohort_order BIGINT,
            intent VARCHAR,
            has_address_reuse BOOLEAN,
            output_type_mismatch BOOLEAN
        )
    ))
),

-- 2) Checkpoint: recompute from 1-day lookback
checkpoint AS (
    SELECT
        COALESCE(MAX(day), DATE '2026-01-01') - INTERVAL '1' DAY AS cutoff_day
    FROM prev
),

-- 3) Raw inputs for date range (non-coinbase only)
raw_inputs AS (
    SELECT
        CAST(date_trunc('day', i.block_time) AS DATE) AS day,
        i.tx_id,
        i.index AS input_index,
        i.block_height,
        i.spent_block_height,
        i.value AS input_value_btc,
        i.address AS input_address,
        i.type AS input_script_type
    FROM bitcoin.inputs i
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', i.block_time) AS DATE) >= c.cutoff_day
      AND CAST(date_trunc('day', i.block_time) AS DATE) < CURRENT_DATE
      AND i.is_coinbase = FALSE
),

-- 4) Raw outputs for date range
raw_outputs AS (
    SELECT
        CAST(date_trunc('day', o.block_time) AS DATE) AS day,
        o.tx_id,
        o.index AS output_index,
        o.value AS output_value_btc,
        o.address AS output_address,
        o.type AS output_script_type
    FROM bitcoin.outputs o
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', o.block_time) AS DATE) >= c.cutoff_day
      AND CAST(date_trunc('day', o.block_time) AS DATE) < CURRENT_DATE
),

-- 5) Aggregate input features per transaction
tx_input_stats AS (
    SELECT
        day,
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
        ) AS avg_days_held,
        -- Collect distinct input addresses for address reuse detection
        ARRAY_AGG(DISTINCT input_address) FILTER (WHERE input_address IS NOT NULL) AS input_addresses,
        -- Count distinct input script types
        COUNT(DISTINCT input_script_type) AS distinct_input_script_count
    FROM raw_inputs
    GROUP BY day, tx_id
),

-- 6) Aggregate output features per transaction
tx_output_stats AS (
    SELECT
        day,
        tx_id,
        COUNT(*) AS output_count,
        SUM(output_value_btc) AS total_output_btc,
        -- Dust detection: outputs < 546 sats = 0.00000546 BTC
        SUM(CASE WHEN output_value_btc < 0.00000546 THEN 1 ELSE 0 END) AS dust_output_count,
        -- Round value detection: divisible by 0.001 BTC
        SUM(CASE WHEN output_value_btc > 0 AND ABS(output_value_btc * 1000 - ROUND(output_value_btc * 1000)) < 0.0000001 THEN 1 ELSE 0 END) AS round_value_count,
        -- Count distinct output script types
        COUNT(DISTINCT output_script_type) AS distinct_output_script_count
    FROM raw_outputs
    GROUP BY day, tx_id
),

-- 7) Detect address reuse: any output address matches any input address
address_reuse_detection AS (
    SELECT DISTINCT
        ri.day,
        ri.tx_id,
        TRUE AS has_address_reuse
    FROM raw_inputs ri
    INNER JOIN raw_outputs ro
        ON ri.day = ro.day
        AND ri.tx_id = ro.tx_id
        AND ri.input_address = ro.output_address
        AND ri.input_address IS NOT NULL
),

-- 8) Combine all transaction features
tx_combined AS (
    SELECT
        i.day,
        i.tx_id,
        i.input_count,
        COALESCE(o.output_count, 0) AS output_count,
        i.total_input_btc,
        COALESCE(o.total_output_btc, 0) AS total_output_btc,
        i.total_input_btc - COALESCE(o.total_output_btc, 0) AS fee_btc,
        COALESCE(o.dust_output_count, 0) AS dust_output_count,
        COALESCE(o.round_value_count, 0) AS round_value_count,
        i.avg_days_held,
        -- Privacy flags
        COALESCE(ar.has_address_reuse, FALSE) AS has_address_reuse,
        -- Output type mismatch (for 2-output tx, different script types)
        (COALESCE(o.output_count, 0) = 2 AND COALESCE(o.distinct_output_script_count, 0) > 1) AS output_type_mismatch,
        -- Derived boolean features for scoring
        i.input_count > 50 AS is_high_fan_in,
        COALESCE(o.output_count, 0) > 50 AS is_high_fan_out,
        COALESCE(o.dust_output_count, 0) > 0 AS has_dust,
        COALESCE(o.round_value_count, 0) > 0 AS has_round_values,
        -- Simple structure: 1-in-1-out or 1-in-2-out
        (i.input_count = 1 AND COALESCE(o.output_count, 0) IN (1, 2)) AS is_simple_structure
    FROM tx_input_stats i
    LEFT JOIN tx_output_stats o
        ON i.day = o.day
        AND i.tx_id = o.tx_id
    LEFT JOIN address_reuse_detection ar
        ON i.day = ar.day
        AND i.tx_id = ar.tx_id
),

-- 9) Calculate human factor score and intent classification
tx_scored AS (
    SELECT
        day,
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
        -- Human factor score calculation
        GREATEST(0, LEAST(100,
            50  -- BASE_SCORE
            -- NEGATIVE INDICATORS (reduce score = more automated)
            + CASE WHEN is_high_fan_in THEN -15 ELSE 0 END
            + CASE WHEN is_high_fan_out THEN -15 ELSE 0 END
            + CASE WHEN has_round_values THEN -5 ELSE 0 END
            + CASE WHEN has_dust THEN -10 ELSE 0 END
            -- POSITIVE INDICATORS (increase score = more human)
            + CASE WHEN is_simple_structure THEN 10 ELSE 0 END
            + CASE WHEN NOT has_round_values THEN 5 ELSE 0 END
            -- BDD-BASED INDICATORS (holding behavior)
            + CASE WHEN avg_days_held >= 1 AND avg_days_held < 365 THEN 10 ELSE 0 END
            + CASE WHEN avg_days_held >= 365 THEN 15 ELSE 0 END
        )) AS human_factor_score,
        -- UTXO Intent classification
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

-- 10) Add score bands and cohort classification
tx_with_bands_and_cohorts AS (
    SELECT
        day,
        tx_id,
        input_count,
        output_count,
        total_input_btc,
        total_output_btc,
        fee_btc,
        dust_output_count,
        round_value_count,
        avg_days_held,
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
        -- Cohort classification based on total input BTC
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
        END AS cohort_order,
        intent,
        has_address_reuse,
        output_type_mismatch
    FROM tx_scored
),

-- 11) New data for incremental processing
new_data AS (
    SELECT
        day,
        tx_id,
        input_count,
        output_count,
        total_input_btc,
        total_output_btc,
        fee_btc,
        dust_output_count,
        round_value_count,
        avg_days_held,
        human_factor_score,
        score_band,
        score_band_order,
        cohort,
        cohort_order,
        intent,
        has_address_reuse,
        output_type_mismatch
    FROM tx_with_bands_and_cohorts
),

-- 12) Keep historical data before cutoff
kept_old AS (
    SELECT p.*
    FROM prev p
    CROSS JOIN checkpoint c
    WHERE p.day < c.cutoff_day
)

-- 13) Final combined result
SELECT * FROM kept_old
UNION ALL
SELECT * FROM new_data
ORDER BY day, tx_id
