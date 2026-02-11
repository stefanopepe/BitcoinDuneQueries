-- ============================================================
-- Smoke Test: Bitcoin Cohort Matrix Drilldown (Zero-Fill)
-- Description: Validates the drilldown query logic that filters
--              the cohort matrix to a single cohort and zero-fills
--              the day × score_band grid for gap-free charting.
--              Tests dense grid completeness, COALESCE correctness,
--              and aggregation invariants.
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
        o.value AS output_value_btc,
        o.type AS output_type,
        o.address AS output_address
    FROM bitcoin.outputs o
    CROSS JOIN block_cutoff bc
    WHERE o.block_height >= bc.min_height
),

-- Transaction fees (input_value - output_value)
tx_fees AS (
    SELECT
        i.tx_id,
        SUM(i.input_value_btc) - COALESCE(SUM(o.output_value_btc), 0) AS fee_btc
    FROM raw_inputs i
    LEFT JOIN raw_outputs o ON i.tx_id = o.tx_id
    GROUP BY i.tx_id
),

-- Privacy flags: address reuse and output type mismatch
tx_privacy AS (
    SELECT
        tx_id,
        COUNT(DISTINCT output_address) < COUNT(*) AS has_address_reuse,
        COUNT(DISTINCT output_type) > 1 AS output_type_mismatch
    FROM raw_outputs
    WHERE output_address IS NOT NULL
    GROUP BY tx_id
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

-- Add bands, cohorts, fees, and privacy flags
tx_final AS (
    SELECT
        s.tx_id,
        s.total_input_btc,
        s.human_factor_score,
        f.fee_btc,
        COALESCE(p.has_address_reuse, FALSE) AS has_address_reuse,
        COALESCE(p.output_type_mismatch, FALSE) AS output_type_mismatch,
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
    FROM tx_scored s
    LEFT JOIN tx_fees f ON s.tx_id = f.tx_id
    LEFT JOIN tx_privacy p ON s.tx_id = p.tx_id
),

-- ============================================================
-- DRILLDOWN-SPECIFIC CTEs
-- Simulate the drilldown query: filter to one cohort + zero-fill
-- Uses 'Shrimps (<1 BTC)' as test cohort (most populated, guaranteed data)
-- ============================================================

-- Sparse matrix: aggregate to (score_band, cohort) like the production query
-- NOTE: no day dimension in smoke test (block range, not date range)
cohort_matrix AS (
    SELECT
        score_band,
        score_band_order,
        cohort,
        cohort_order,
        COUNT(*) AS tx_count,
        SUM(total_input_btc) AS btc_volume,
        AVG(human_factor_score) AS avg_score,
        AVG(fee_btc) AS avg_fee_btc,
        SUM(fee_btc) AS total_fee_btc,
        SUM(CASE WHEN has_address_reuse THEN 1 ELSE 0 END) AS tx_with_address_reuse,
        SUM(CASE WHEN output_type_mismatch THEN 1 ELSE 0 END) AS tx_with_output_mismatch,
        ROUND(100.0 * SUM(CASE WHEN has_address_reuse THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_address_reuse
    FROM tx_final
    GROUP BY score_band, score_band_order, cohort, cohort_order
),

-- Zero-fill spine: all 10 score bands
score_band_spine AS (
    SELECT score_band, score_band_order
    FROM (VALUES
        ('0-10', 1), ('10-20', 2), ('20-30', 3), ('30-40', 4), ('40-50', 5),
        ('50-60', 6), ('60-70', 7), ('70-80', 8), ('80-90', 9), ('90-100', 10)
    ) AS t(score_band, score_band_order)
),

-- Apply drilldown: filter to Shrimps + LEFT JOIN to spine
drilldown AS (
    SELECT
        sb.score_band,
        sb.score_band_order,
        'Shrimps (<1 BTC)' AS cohort,
        1 AS cohort_order,
        -- Counts and sums: COALESCE to 0
        COALESCE(m.tx_count, 0) AS tx_count,
        COALESCE(m.btc_volume, 0.0) AS btc_volume,
        -- Averages: NULL for missing cells (avg of zero obs is undefined)
        m.avg_score,
        -- Fee metrics
        COALESCE(m.avg_fee_btc, 0.0) AS avg_fee_btc,
        COALESCE(m.total_fee_btc, 0.0) AS total_fee_btc,
        -- Privacy counts: COALESCE to 0
        COALESCE(m.tx_with_address_reuse, 0) AS tx_with_address_reuse,
        COALESCE(m.tx_with_output_mismatch, 0) AS tx_with_output_mismatch,
        -- Percentage: NULL for missing cells
        m.pct_address_reuse
    FROM score_band_spine sb
    LEFT JOIN cohort_matrix m
        ON sb.score_band_order = m.score_band_order
        AND m.cohort = 'Shrimps (<1 BTC)'
)

-- ============================================================
-- VALIDATION QUERIES - Uncomment one at a time to test
-- ============================================================

-- 1. Dense Grid Completeness (primary validation)
--    Verifies zero-fill produces exactly 10 rows with correct NULL semantics
-- SELECT
--     COUNT(*) AS total_rows,
--     COUNT(DISTINCT score_band) AS distinct_bands,
--     SUM(CASE WHEN tx_count > 0 THEN 1 ELSE 0 END) AS populated_rows,
--     SUM(CASE WHEN tx_count = 0 THEN 1 ELSE 0 END) AS zero_filled_rows,
--     -- NULL invariant: populated rows must have non-NULL avg_score
--     SUM(CASE WHEN tx_count > 0 AND avg_score IS NULL THEN 1 ELSE 0 END) AS err_missing_avg_score,
--     -- NULL invariant: zero-filled rows must have NULL avg_score
--     SUM(CASE WHEN tx_count = 0 AND avg_score IS NOT NULL THEN 1 ELSE 0 END) AS err_false_avg_score,
--     -- NULL invariant: zero-filled rows must have NULL pct_address_reuse
--     SUM(CASE WHEN tx_count = 0 AND pct_address_reuse IS NOT NULL THEN 1 ELSE 0 END) AS err_false_pct_reuse,
--     -- All rows must have cohort = 'Shrimps (<1 BTC)'
--     COUNT(DISTINCT cohort) AS distinct_cohorts,
--     MIN(cohort_order) AS min_cohort_order,
--     MAX(cohort_order) AS max_cohort_order
-- FROM drilldown
-- EXPECTED:
--   total_rows = 10
--   distinct_bands = 10
--   populated_rows + zero_filled_rows = 10
--   err_missing_avg_score = 0
--   err_false_avg_score = 0
--   err_false_pct_reuse = 0
--   distinct_cohorts = 1
--   min_cohort_order = 1, max_cohort_order = 1

-- 2. Full Drilldown Output (uncomment to see all 10 rows)
-- SELECT * FROM drilldown ORDER BY score_band_order

-- 3. Aggregation Invariant: drilldown totals must match sparse matrix
--    for the same cohort
SELECT
    'drilldown' AS source,
    SUM(tx_count) AS total_tx,
    ROUND(SUM(btc_volume), 8) AS total_btc,
    ROUND(SUM(total_fee_btc), 8) AS total_fees,
    SUM(tx_with_address_reuse) AS total_addr_reuse,
    SUM(tx_with_output_mismatch) AS total_output_mismatch
FROM drilldown
UNION ALL
SELECT
    'sparse_matrix' AS source,
    SUM(tx_count) AS total_tx,
    ROUND(SUM(btc_volume), 8) AS total_btc,
    ROUND(SUM(total_fee_btc), 8) AS total_fees,
    SUM(tx_with_address_reuse) AS total_addr_reuse,
    SUM(tx_with_output_mismatch) AS total_output_mismatch
FROM cohort_matrix
WHERE cohort = 'Shrimps (<1 BTC)'
-- EXPECTED: both rows have identical values for all columns

-- 4. Column Presence Check (uncomment to verify all 13 output columns)
-- SELECT
--     score_band, score_band_order, cohort, cohort_order,
--     tx_count, btc_volume, avg_score,
--     avg_fee_btc, total_fee_btc,
--     tx_with_address_reuse, tx_with_output_mismatch, pct_address_reuse
-- FROM drilldown
-- LIMIT 1
-- (If this runs without error, all columns exist)

-- 5. Score Band Ordering Check (verify monotonic ordering)
-- SELECT
--     score_band,
--     score_band_order,
--     tx_count,
--     LAG(score_band_order) OVER (ORDER BY score_band_order) AS prev_order
-- FROM drilldown
-- ORDER BY score_band_order
-- EXPECTED: score_band_order runs 1 through 10, prev_order is always current - 1

-- 6. Zero-Fill COALESCE Correctness (verify 0 vs NULL for empty cells)
-- SELECT
--     score_band,
--     tx_count,
--     btc_volume,
--     avg_score,
--     avg_fee_btc,
--     total_fee_btc,
--     tx_with_address_reuse,
--     tx_with_output_mismatch,
--     pct_address_reuse,
--     -- Check: if tx_count=0, sums must be 0 and avgs must be NULL
--     CASE
--         WHEN tx_count = 0 AND btc_volume != 0 THEN 'ERR: btc_volume'
--         WHEN tx_count = 0 AND total_fee_btc != 0 THEN 'ERR: total_fee_btc'
--         WHEN tx_count = 0 AND tx_with_address_reuse != 0 THEN 'ERR: addr_reuse'
--         WHEN tx_count = 0 AND tx_with_output_mismatch != 0 THEN 'ERR: output_mismatch'
--         WHEN tx_count = 0 AND avg_score IS NOT NULL THEN 'ERR: avg_score not NULL'
--         WHEN tx_count = 0 AND pct_address_reuse IS NOT NULL THEN 'ERR: pct_reuse not NULL'
--         ELSE 'OK'
--     END AS zero_fill_check
-- FROM drilldown
-- ORDER BY score_band_order
-- EXPECTED: all rows show 'OK'

-- ============================================================
-- NOTE: This smoke test uses a single block range (no day dimension)
-- unlike the production query which operates on day × score_band.
-- The zero-fill spine here is 1D (score_bands only).
-- Production uses 2D (days × score_bands via SEQUENCE).
-- ============================================================
