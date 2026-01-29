-- ============================================================
-- Smoke Test: Bitcoin Privacy Heuristics V2
-- Description: Quick validation query to test the privacy heuristics
--              logic on a small sample of recent transactions.
-- Usage: Copy/paste to Dune and run. Should complete in <30 seconds.
-- ============================================================

WITH
-- Sample recent data (last 1 hour for speed)
raw_inputs AS (
    SELECT
        CAST(date_trunc('day', i.block_time) AS DATE) AS day,
        i.tx_id,
        i.index AS input_index,
        i.value AS input_value,
        i.address AS input_address,
        i.type AS input_script_type
    FROM bitcoin.inputs i
    WHERE i.block_time >= NOW() - INTERVAL '1' HOUR
      AND i.is_coinbase = FALSE
),

raw_outputs AS (
    SELECT
        CAST(date_trunc('day', o.block_time) AS DATE) AS day,
        o.tx_id,
        o.index AS output_index,
        o.value AS output_value,
        o.address AS output_address,
        o.type AS output_script_type
    FROM bitcoin.outputs o
    WHERE o.block_time >= NOW() - INTERVAL '1' HOUR
      AND o.type NOT IN ('nulldata', 'nonstandard')
),

tx_input_stats AS (
    SELECT
        day,
        tx_id,
        COUNT(*) AS input_count,
        SUM(input_value) AS total_input_value,
        MIN(input_value) AS min_input_value,
        MAX(input_value) AS max_input_value,
        ARRAY_AGG(DISTINCT input_script_type) AS input_script_types,
        COUNT(DISTINCT input_script_type) AS distinct_input_script_count
    FROM raw_inputs
    GROUP BY day, tx_id
),

tx_output_stats AS (
    SELECT
        day,
        tx_id,
        COUNT(*) AS output_count,
        SUM(output_value) AS total_output_value,
        MIN(output_value) AS min_output_value,
        MAX(output_value) AS max_output_value,
        ARRAY_AGG(DISTINCT output_script_type) AS output_script_types,
        COUNT(DISTINCT output_script_type) AS distinct_output_script_count
    FROM raw_outputs
    GROUP BY day, tx_id
),

two_output_details AS (
    SELECT
        day,
        tx_id,
        ARRAY_AGG(output_value ORDER BY output_index) AS output_values,
        ARRAY_AGG(output_script_type ORDER BY output_index) AS output_types
    FROM raw_outputs
    WHERE tx_id IN (
        SELECT tx_id FROM tx_output_stats WHERE output_count = 2
    )
    GROUP BY day, tx_id
),

coinjoin_detection AS (
    SELECT
        o.day,
        o.tx_id,
        COUNT(*) AS total_outputs,
        MAX(value_count) AS max_equal_outputs
    FROM raw_outputs o
    INNER JOIN (
        SELECT day, tx_id, output_value, COUNT(*) AS value_count
        FROM raw_outputs
        GROUP BY day, tx_id, output_value
    ) vc ON o.day = vc.day AND o.tx_id = vc.tx_id
    GROUP BY o.day, o.tx_id
),

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
        COALESCE(cj.total_outputs, 0) AS cj_total_outputs,
        COALESCE(cj.max_equal_outputs, 0) AS cj_max_equal_outputs,
        COALESCE(ar.has_address_reuse, FALSE) AS has_address_reuse
    FROM tx_input_stats i
    LEFT JOIN tx_output_stats o ON i.day = o.day AND i.tx_id = o.tx_id
    LEFT JOIN two_output_details tod ON i.day = tod.day AND i.tx_id = tod.tx_id
    LEFT JOIN coinjoin_detection cj ON i.day = cj.day AND i.tx_id = cj.tx_id
    LEFT JOIN address_reuse_detection ar ON i.day = ar.day AND i.tx_id = ar.tx_id
),

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

classified AS (
    SELECT
        day,
        tx_id,
        input_count,
        output_count,
        total_input_value,
        CASE
            WHEN output_count = 0 THEN 'malformed'
            WHEN input_count >= 2
                 AND cj_total_outputs >= 2
                 AND cj_max_equal_outputs >= 2
                 AND CAST(cj_max_equal_outputs AS DOUBLE) / CAST(cj_total_outputs AS DOUBLE) >= 0.5
                 AND cj_max_equal_outputs >= LEAST(GREATEST(2, cj_total_outputs / 2), 5)
            THEN 'coinjoin_detected'
            WHEN output_count = 1 THEN 'self_transfer'
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
    privacy_heuristic,
    COUNT(*) AS tx_count,
    SUM(total_input_value) AS sats_total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM classified
GROUP BY privacy_heuristic
ORDER BY tx_count DESC;
