-- ============================================================
-- SMOKE TEST: Bitcoin Privacy Heuristics V2
-- Purpose: Validate query logic on a small date range
-- Usage: Run on Dune to verify no syntax errors and reasonable output
-- ============================================================

WITH
-- Use fixed date range for testing (2 days)
test_params AS (
    SELECT
        DATE '2026-01-27' AS start_date,
        DATE '2026-01-29' AS end_date
),

-- 1) Get all non-coinbase inputs with their details
raw_inputs AS (
    SELECT
        CAST(date_trunc('day', i.block_time) AS DATE) AS day,
        i.tx_id,
        i.index AS input_index,
        i.value AS input_value,
        i.address AS input_address,
        i.type AS input_script_type
    FROM bitcoin.inputs i
    CROSS JOIN test_params p
    WHERE CAST(date_trunc('day', i.block_time) AS DATE) >= p.start_date
      AND CAST(date_trunc('day', i.block_time) AS DATE) < p.end_date
      AND i.is_coinbase = FALSE
),

-- 2) Get all outputs with their details
raw_outputs AS (
    SELECT
        CAST(date_trunc('day', o.block_time) AS DATE) AS day,
        o.tx_id,
        o.index AS output_index,
        o.value AS output_value,
        o.address AS output_address,
        o.type AS output_script_type
    FROM bitcoin.outputs o
    CROSS JOIN test_params p
    WHERE CAST(date_trunc('day', o.block_time) AS DATE) >= p.start_date
      AND CAST(date_trunc('day', o.block_time) AS DATE) < p.end_date
),

-- 3) Get tx-level input/output counts for UTXO classification
tx_counts AS (
    SELECT
        i.day,
        i.tx_id,
        COUNT(DISTINCT i.input_index) AS input_count,
        COUNT(DISTINCT o.output_index) AS output_count
    FROM raw_inputs i
    LEFT JOIN raw_outputs o ON i.day = o.day AND i.tx_id = o.tx_id
    GROUP BY i.day, i.tx_id
),

-- 4) Filter to "other" intent (exclude all UTXO-classified categories)
-- Mirrors classification logic from query_6614095 (Bitcoin UTXO Heuristics)
other_tx_ids AS (
    SELECT day, tx_id, input_count, output_count
    FROM tx_counts
    WHERE NOT (
        (input_count >= 10 AND output_count <= 2)                           -- consolidation
        OR (input_count <= 2 AND output_count >= 10)                        -- fan_out_batch
        OR (input_count >= 5 AND output_count >= 5
            AND ABS(input_count - output_count) <= 1)                       -- coinjoin_like
        OR (input_count = 1 AND output_count = 1)                           -- self_transfer
        OR (output_count = 2 AND input_count >= 2)                          -- change_like_2_outputs
        OR (output_count = 0)                                               -- malformed_no_outputs
    )
),

-- 5) Aggregate transaction-level input stats (filtered to "other" only)
tx_input_stats AS (
    SELECT
        ri.day,
        ri.tx_id,
        COUNT(*) AS input_count,
        SUM(ri.input_value) AS total_input_value,
        MIN(ri.input_value) AS min_input_value,
        MAX(ri.input_value) AS max_input_value,
        ARRAY_AGG(DISTINCT ri.input_script_type) AS input_script_types,
        COUNT(DISTINCT ri.input_script_type) AS distinct_input_script_count
    FROM raw_inputs ri
    INNER JOIN other_tx_ids o ON ri.day = o.day AND ri.tx_id = o.tx_id
    GROUP BY ri.day, ri.tx_id
),

-- 6) Aggregate transaction-level output stats (filtered to "other" only)
tx_output_stats AS (
    SELECT
        ro.day,
        ro.tx_id,
        COUNT(*) AS output_count,
        SUM(ro.output_value) AS total_output_value,
        MIN(ro.output_value) AS min_output_value,
        MAX(ro.output_value) AS max_output_value,
        ARRAY_AGG(DISTINCT ro.output_script_type) AS output_script_types,
        COUNT(DISTINCT ro.output_script_type) AS distinct_output_script_count
    FROM raw_outputs ro
    INNER JOIN other_tx_ids o ON ro.day = o.day AND ro.tx_id = o.tx_id
    GROUP BY ro.day, ro.tx_id
),

-- 7) For 2-output transactions, get individual output details for precision analysis
two_output_details AS (
    SELECT
        ro.day,
        ro.tx_id,
        ARRAY_AGG(ro.output_value ORDER BY ro.output_index) AS output_values,
        ARRAY_AGG(ro.output_script_type ORDER BY ro.output_index) AS output_types
    FROM raw_outputs ro
    INNER JOIN other_tx_ids o ON ro.day = o.day AND ro.tx_id = o.tx_id
    WHERE o.output_count = 2
    GROUP BY ro.day, ro.tx_id
),

-- 8) Detect address reuse: output address matches input address (filtered to "other" only)
address_reuse_detection AS (
    SELECT DISTINCT
        ri.day,
        ri.tx_id,
        TRUE AS has_address_reuse
    FROM raw_inputs ri
    INNER JOIN other_tx_ids ot ON ri.day = ot.day AND ri.tx_id = ot.tx_id
    INNER JOIN raw_outputs ro
        ON ri.day = ro.day
        AND ri.tx_id = ro.tx_id
        AND ri.input_address = ro.output_address
        AND ri.input_address IS NOT NULL
),

-- 9) Combine all data for classification
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
        i.total_input_value - COALESCE(o.total_output_value, 0) AS fee,
        tod.output_values,
        tod.output_types,
        COALESCE(ar.has_address_reuse, FALSE) AS has_address_reuse
    FROM tx_input_stats i
    LEFT JOIN tx_output_stats o ON i.day = o.day AND i.tx_id = o.tx_id
    LEFT JOIN two_output_details tod ON i.day = tod.day AND i.tx_id = tod.tx_id
    LEFT JOIN address_reuse_detection ar ON i.day = ar.day AND i.tx_id = ar.tx_id
),

-- 10) Calculate precision for 2-output transactions
tx_with_precision AS (
    SELECT
        *,
        CASE
            WHEN output_count = 2 AND output_values IS NOT NULL THEN
                ABS(
                    COALESCE(LENGTH(CAST(output_values[1] AS VARCHAR))
                        - LENGTH(RTRIM(CAST(output_values[1] AS VARCHAR), '0')), 0)
                    -
                    COALESCE(LENGTH(CAST(output_values[2] AS VARCHAR))
                        - LENGTH(RTRIM(CAST(output_values[2] AS VARCHAR), '0')), 0)
                )
            ELSE 0
        END AS precision_diff,
        CASE
            WHEN output_count = 2 AND output_types IS NOT NULL
                 AND output_types[1] != output_types[2] THEN TRUE
            ELSE FALSE
        END AS script_types_differ
    FROM tx_combined
),

-- 11) Apply all privacy heuristics
classified AS (
    SELECT
        day,
        tx_id,
        input_count,
        output_count,
        total_input_value,
        total_output_value,
        CASE
            WHEN output_count = 0 THEN 'malformed'
            WHEN output_count = 2 AND precision_diff >= 3 THEN 'change_precision'
            WHEN output_count = 2
                 AND script_types_differ
                 AND distinct_input_script_count = 1
            THEN 'change_script_type'
            WHEN input_count >= 2
                 AND output_count = 2
                 AND distinct_input_script_count > 1
                 AND (total_input_value - min_input_value) >= (max_output_value + fee)
            THEN 'uih2'
            WHEN input_count >= 2
                 AND output_count = 2
                 AND distinct_input_script_count > 1
                 AND (total_input_value - min_input_value) >= (min_output_value + fee)
            THEN 'uih1'
            WHEN has_address_reuse THEN 'address_reuse'
            ELSE 'no_privacy_issues'
        END AS privacy_heuristic
    FROM tx_with_precision
)

-- Final aggregation
SELECT
    day,
    privacy_heuristic,
    COUNT(*) AS tx_count,
    SUM(total_input_value) AS sats_total,
    AVG(input_count) AS avg_inputs,
    AVG(output_count) AS avg_outputs
FROM classified
GROUP BY day, privacy_heuristic
ORDER BY day, privacy_heuristic;
