-- ============================================================
-- Query: Bitcoin Privacy Heuristics V2
-- Description: Implements advanced privacy analysis heuristics based on
--              Blockstream Esplora's privacy-analysis.js methodology.
--              Detects: change outputs (precision/script mismatch), UIH,
--              CoinJoin patterns, self-transfers, and address reuse.
-- Author: stefanopepe
-- Created: 2026-01-29
-- Updated: 2026-01-29
-- Reference: https://github.com/Blockstream/esplora/blob/master/client/src/lib/privacy-analysis.js
-- Note: Uses incremental processing with 1-day lookback.
-- ============================================================
-- Privacy Heuristics Implemented:
--   1. change_precision    - Change detected via decimal precision difference (≥3 digits)
--   2. change_script_type  - Change detected via script type mismatch
--   3. uih1                - Unnecessary Input Heuristic 1 (smallest input covers smallest output)
--   4. uih2                - Unnecessary Input Heuristic 2 (smallest input covers largest output)
--   5. coinjoin_detected   - CoinJoin pattern (≥50% equal outputs, 2-5+ matching)
--   6. self_transfer       - Single output, no change (wallet consolidation/self-send)
--   7. address_reuse       - Output script matches an input script
--   8. no_privacy_issues   - No heuristics triggered
-- ============================================================
-- Output Columns:
--   day                   - Date of transactions
--   privacy_heuristic     - The privacy issue detected
--   tx_count              - Number of transactions
--   sats_total            - Total satoshis involved
--   avg_inputs            - Average input count
--   avg_outputs           - Average output count
-- ============================================================

WITH
-- 1) Previous results (empty on first ever run)
prev AS (
    SELECT *
    FROM TABLE(previous.query.result(
        schema => DESCRIPTOR(
            day DATE,
            privacy_heuristic VARCHAR,
            tx_count BIGINT,
            sats_total DOUBLE,
            avg_inputs DOUBLE,
            avg_outputs DOUBLE
        )
    ))
),

-- 2) Checkpoint: recompute from 1-day lookback
checkpoint AS (
    SELECT
        COALESCE(MAX(day), DATE '2026-01-01') - INTERVAL '1' DAY AS cutoff_day
    FROM prev
),

-- 3) Get all non-coinbase inputs with their details
raw_inputs AS (
    SELECT
        CAST(date_trunc('day', i.block_time) AS DATE) AS day,
        i.tx_id,
        i.index AS input_index,
        i.value AS input_value,
        i.address AS input_address,
        i.type AS input_script_type
    FROM bitcoin.inputs i
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', i.block_time) AS DATE) >= c.cutoff_day
      AND CAST(date_trunc('day', i.block_time) AS DATE) < CURRENT_DATE
      AND i.is_coinbase = FALSE
),

-- 4) Get all outputs with their details
raw_outputs AS (
    SELECT
        CAST(date_trunc('day', o.block_time) AS DATE) AS day,
        o.tx_id,
        o.index AS output_index,
        o.value AS output_value,
        o.address AS output_address,
        o.type AS output_script_type
    FROM bitcoin.outputs o
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', o.block_time) AS DATE) >= c.cutoff_day
      AND CAST(date_trunc('day', o.block_time) AS DATE) < CURRENT_DATE
),

-- 5) Aggregate transaction-level input stats
tx_input_stats AS (
    SELECT
        day,
        tx_id,
        COUNT(*) AS input_count,
        SUM(input_value) AS total_input_value,
        MIN(input_value) AS min_input_value,
        MAX(input_value) AS max_input_value,
        -- Collect distinct input script types
        ARRAY_AGG(DISTINCT input_script_type) AS input_script_types,
        -- Check if all inputs have same script type (for UIH exclusion)
        COUNT(DISTINCT input_script_type) AS distinct_input_script_count
    FROM raw_inputs
    GROUP BY day, tx_id
),

-- 6) Aggregate transaction-level output stats
tx_output_stats AS (
    SELECT
        day,
        tx_id,
        COUNT(*) AS output_count,
        SUM(output_value) AS total_output_value,
        MIN(output_value) AS min_output_value,
        MAX(output_value) AS max_output_value,
        -- Collect distinct output script types
        ARRAY_AGG(DISTINCT output_script_type) AS output_script_types,
        COUNT(DISTINCT output_script_type) AS distinct_output_script_count
    FROM raw_outputs
    GROUP BY day, tx_id
),

-- 7) For 2-output transactions, get individual output details for precision analysis
two_output_details AS (
    SELECT
        day,
        tx_id,
        ARRAY_AGG(output_value ORDER BY output_index) AS output_values,
        ARRAY_AGG(output_script_type ORDER BY output_index) AS output_types
    FROM raw_outputs
    WHERE tx_id IN (
        SELECT tx_id
        FROM tx_output_stats
        WHERE output_count = 2
    )
    GROUP BY day, tx_id
),

-- 8) Detect CoinJoin patterns: multiple equal outputs
coinjoin_detection AS (
    SELECT
        o.day,
        o.tx_id,
        COUNT(*) AS total_outputs,
        -- Count outputs with matching values
        MAX(value_count) AS max_equal_outputs
    FROM raw_outputs o
    INNER JOIN (
        -- Find the most common output value per tx
        SELECT
            day,
            tx_id,
            output_value,
            COUNT(*) AS value_count
        FROM raw_outputs
        GROUP BY day, tx_id, output_value
    ) vc ON o.day = vc.day AND o.tx_id = vc.tx_id
    GROUP BY o.day, o.tx_id
),

-- 9) Detect address reuse: output address matches input address
address_reuse_detection AS (
    SELECT DISTINCT
        i.day,
        i.tx_id,
        TRUE AS has_address_reuse
    FROM raw_inputs i
    INNER JOIN raw_outputs o
        ON i.day = o.day
        AND i.tx_id = o.tx_id
        AND i.input_address = o.output_address
        AND i.input_address IS NOT NULL
),

-- 10) Combine all data for classification
tx_combined AS (
    SELECT
        i.day,
        i.tx_id,
        i.input_count,
        i.total_input_value,
        i.min_input_value,
        i.max_input_value,
        i.input_script_types,
        i.distinct_input_script_count,
        COALESCE(o.output_count, 0) AS output_count,
        COALESCE(o.total_output_value, 0) AS total_output_value,
        COALESCE(o.min_output_value, 0) AS min_output_value,
        COALESCE(o.max_output_value, 0) AS max_output_value,
        o.output_script_types,
        COALESCE(o.distinct_output_script_count, 0) AS distinct_output_script_count,
        -- Fee calculation
        i.total_input_value - COALESCE(o.total_output_value, 0) AS fee,
        -- Two-output details
        tod.output_values,
        tod.output_types,
        -- CoinJoin metrics
        COALESCE(cj.total_outputs, 0) AS cj_total_outputs,
        COALESCE(cj.max_equal_outputs, 0) AS cj_max_equal_outputs,
        -- Address reuse
        COALESCE(ar.has_address_reuse, FALSE) AS has_address_reuse
    FROM tx_input_stats i
    LEFT JOIN tx_output_stats o ON i.day = o.day AND i.tx_id = o.tx_id
    LEFT JOIN two_output_details tod ON i.day = tod.day AND i.tx_id = tod.tx_id
    LEFT JOIN coinjoin_detection cj ON i.day = cj.day AND i.tx_id = cj.tx_id
    LEFT JOIN address_reuse_detection ar ON i.day = ar.day AND i.tx_id = ar.tx_id
),

-- 11) Calculate precision for 2-output transactions
-- Precision = number of trailing zeros when expressed in satoshis
tx_with_precision AS (
    SELECT
        *,
        -- For 2-output txs, calculate precision difference
        CASE
            WHEN output_count = 2 AND output_values IS NOT NULL THEN
                ABS(
                    -- Count trailing zeros for first output
                    COALESCE(LENGTH(CAST(output_values[1] AS VARCHAR))
                        - LENGTH(RTRIM(CAST(output_values[1] AS VARCHAR), '0')), 0)
                    -
                    -- Count trailing zeros for second output
                    COALESCE(LENGTH(CAST(output_values[2] AS VARCHAR))
                        - LENGTH(RTRIM(CAST(output_values[2] AS VARCHAR), '0')), 0)
                )
            ELSE 0
        END AS precision_diff,
        -- Check if output script types differ
        CASE
            WHEN output_count = 2 AND output_types IS NOT NULL
                 AND output_types[1] != output_types[2] THEN TRUE
            ELSE FALSE
        END AS script_types_differ
    FROM tx_combined
),

-- 12) Apply all privacy heuristics
classified AS (
    SELECT
        day,
        tx_id,
        input_count,
        output_count,
        total_input_value,
        total_output_value,
        CASE
            -- Skip malformed transactions
            WHEN output_count = 0 THEN 'malformed'

            -- CoinJoin Detection: ≥50% of outputs are equal, with at least 2-5 matching
            -- Multiple inputs required
            WHEN input_count >= 2
                 AND cj_total_outputs >= 2
                 AND cj_max_equal_outputs >= 2
                 AND CAST(cj_max_equal_outputs AS DOUBLE) / CAST(cj_total_outputs AS DOUBLE) >= 0.5
                 AND cj_max_equal_outputs >= LEAST(GREATEST(2, cj_total_outputs / 2), 5)
            THEN 'coinjoin_detected'

            -- Self-Transfer: Single output (no change), potential consolidation
            WHEN output_count = 1 THEN 'self_transfer'

            -- For 2-output transactions, apply change detection heuristics
            -- Change via Precision Loss: ≥3 digit difference in trailing zeros
            WHEN output_count = 2 AND precision_diff >= 3 THEN 'change_precision'

            -- Change via Script Type Mismatch: different output types, one matches inputs
            WHEN output_count = 2
                 AND script_types_differ
                 AND distinct_input_script_count = 1
            THEN 'change_script_type'

            -- UIH2: Smallest input unnecessary for largest output + fee
            -- (exotic transaction motive)
            WHEN input_count >= 2
                 AND output_count = 2
                 AND distinct_input_script_count > 1  -- Skip if all inputs same type
                 AND (total_input_value - min_input_value) >= (max_output_value + fee)
            THEN 'uih2'

            -- UIH1: Smallest input unnecessary for smallest output + fee
            -- (smallest output is likely change)
            WHEN input_count >= 2
                 AND output_count = 2
                 AND distinct_input_script_count > 1  -- Skip if all inputs same type
                 AND (total_input_value - min_input_value) >= (min_output_value + fee)
            THEN 'uih1'

            -- Address Reuse: Output goes to an address that was an input
            WHEN has_address_reuse THEN 'address_reuse'

            -- No privacy issues detected
            ELSE 'no_privacy_issues'
        END AS privacy_heuristic
    FROM tx_with_precision
),

-- 13) Aggregate by day and heuristic
new_data AS (
    SELECT
        day,
        privacy_heuristic,
        COUNT(*) AS tx_count,
        SUM(total_input_value) AS sats_total,
        AVG(input_count) AS avg_inputs,
        AVG(output_count) AS avg_outputs
    FROM classified
    GROUP BY day, privacy_heuristic
),

-- 14) Keep historical data before cutoff
kept_old AS (
    SELECT p.*
    FROM prev p
    CROSS JOIN checkpoint c
    WHERE p.day < c.cutoff_day
)

-- 15) Final combined result
SELECT * FROM kept_old
UNION ALL
SELECT * FROM new_data
ORDER BY day, privacy_heuristic;
