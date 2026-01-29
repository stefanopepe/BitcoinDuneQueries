-- ============================================================
-- Smoke Test: Bitcoin Privacy Heuristics V2
-- Description: Quick validation query to test the privacy heuristics
--              logic on a small sample of recent transactions.
-- Usage: Copy/paste to Dune and run. Should complete in <30 seconds.
-- ============================================================

WITH
-- Get cutoff block height (last 100 blocks)
block_cutoff AS (
    SELECT MAX(height) - 100 AS min_height FROM bitcoin.blocks
),

-- Sample recent data (last 100 blocks for reliable data)
raw_inputs AS (
    SELECT
        i.tx_id,
        i.index AS input_index,
        i.value AS input_value,
        i.address AS input_address,
        i.type AS input_script_type
    FROM bitcoin.inputs i
    CROSS JOIN block_cutoff bc
    WHERE i.block_height >= bc.min_height
      AND i.is_coinbase = FALSE
),

raw_outputs AS (
    SELECT
        o.tx_id,
        o.index AS output_index,
        o.value AS output_value,
        o.address AS output_address,
        o.type AS output_script_type
    FROM bitcoin.outputs o
    CROSS JOIN block_cutoff bc
    WHERE o.block_height >= bc.min_height
      AND o.type NOT IN ('nulldata', 'nonstandard', 'unknown')
),

tx_input_stats AS (
    SELECT
        tx_id,
        COUNT(*) AS input_count,
        SUM(input_value) AS total_input_value,
        MIN(input_value) AS min_input_value,
        MAX(input_value) AS max_input_value,
        COUNT(DISTINCT input_script_type) AS distinct_input_script_count
    FROM raw_inputs
    GROUP BY tx_id
),

tx_output_stats AS (
    SELECT
        tx_id,
        COUNT(*) AS output_count,
        SUM(output_value) AS total_output_value,
        MIN(output_value) AS min_output_value,
        MAX(output_value) AS max_output_value,
        COUNT(DISTINCT output_script_type) AS distinct_output_script_count
    FROM raw_outputs
    GROUP BY tx_id
),

-- For 2-output txs: get the two output values and types
two_output_txs AS (
    SELECT
        o.tx_id,
        MIN(CASE WHEN rn = 1 THEN output_value END) AS out1_value,
        MIN(CASE WHEN rn = 2 THEN output_value END) AS out2_value,
        MIN(CASE WHEN rn = 1 THEN output_script_type END) AS out1_type,
        MIN(CASE WHEN rn = 2 THEN output_script_type END) AS out2_type
    FROM (
        SELECT
            tx_id,
            output_value,
            output_script_type,
            ROW_NUMBER() OVER (PARTITION BY tx_id ORDER BY output_index) AS rn
        FROM raw_outputs
        WHERE tx_id IN (SELECT tx_id FROM tx_output_stats WHERE output_count = 2)
    ) o
    GROUP BY o.tx_id
),

-- Count equal outputs for coinjoin detection
coinjoin_detection AS (
    SELECT
        tx_id,
        COUNT(*) AS total_outputs,
        MAX(value_count) AS max_equal_outputs
    FROM (
        SELECT tx_id, output_value, COUNT(*) AS value_count
        FROM raw_outputs
        GROUP BY tx_id, output_value
    )
    GROUP BY tx_id
),

-- Detect address reuse
address_reuse_detection AS (
    SELECT DISTINCT i.tx_id
    FROM raw_inputs i
    INNER JOIN raw_outputs o
        ON i.tx_id = o.tx_id
        AND i.input_address = o.output_address
        AND i.input_address IS NOT NULL
),

-- Combine all stats
tx_combined AS (
    SELECT
        i.tx_id,
        i.input_count,
        i.total_input_value,
        i.min_input_value,
        i.max_input_value,
        i.distinct_input_script_count,
        COALESCE(o.output_count, 0) AS output_count,
        COALESCE(o.total_output_value, 0) AS total_output_value,
        COALESCE(o.min_output_value, 0) AS min_output_value,
        COALESCE(o.max_output_value, 0) AS max_output_value,
        COALESCE(o.distinct_output_script_count, 0) AS distinct_output_script_count,
        i.total_input_value - COALESCE(o.total_output_value, 0) AS fee,
        t2.out1_value,
        t2.out2_value,
        t2.out1_type,
        t2.out2_type,
        COALESCE(cj.total_outputs, 0) AS cj_total_outputs,
        COALESCE(cj.max_equal_outputs, 0) AS cj_max_equal_outputs,
        CASE WHEN ar.tx_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_address_reuse
    FROM tx_input_stats i
    LEFT JOIN tx_output_stats o ON i.tx_id = o.tx_id
    LEFT JOIN two_output_txs t2 ON i.tx_id = t2.tx_id
    LEFT JOIN coinjoin_detection cj ON i.tx_id = cj.tx_id
    LEFT JOIN address_reuse_detection ar ON i.tx_id = ar.tx_id
),

-- Calculate precision difference using log10 for trailing zeros
-- trailing zeros ≈ number of times divisible by 10
tx_with_precision AS (
    SELECT
        *,
        CASE
            WHEN output_count = 2 AND out1_value IS NOT NULL AND out2_value IS NOT NULL
                 AND out1_value > 0 AND out2_value > 0 THEN
                ABS(
                    -- Count trailing zeros: floor(log10(gcd(value, 10^8))) approximation
                    -- Simplified: compare if values are "round" (divisible by 1000+ sats)
                    CASE WHEN out1_value % 100000000 = 0 THEN 8
                         WHEN out1_value % 10000000 = 0 THEN 7
                         WHEN out1_value % 1000000 = 0 THEN 6
                         WHEN out1_value % 100000 = 0 THEN 5
                         WHEN out1_value % 10000 = 0 THEN 4
                         WHEN out1_value % 1000 = 0 THEN 3
                         WHEN out1_value % 100 = 0 THEN 2
                         WHEN out1_value % 10 = 0 THEN 1
                         ELSE 0 END
                    -
                    CASE WHEN out2_value % 100000000 = 0 THEN 8
                         WHEN out2_value % 10000000 = 0 THEN 7
                         WHEN out2_value % 1000000 = 0 THEN 6
                         WHEN out2_value % 100000 = 0 THEN 5
                         WHEN out2_value % 10000 = 0 THEN 4
                         WHEN out2_value % 1000 = 0 THEN 3
                         WHEN out2_value % 100 = 0 THEN 2
                         WHEN out2_value % 10 = 0 THEN 1
                         ELSE 0 END
                )
            ELSE 0
        END AS precision_diff,
        CASE
            WHEN output_count = 2 AND out1_type IS NOT NULL AND out2_type IS NOT NULL
                 AND out1_type != out2_type THEN TRUE
            ELSE FALSE
        END AS script_types_differ
    FROM tx_combined
),

-- Classify transactions
classified AS (
    SELECT
        tx_id,
        input_count,
        output_count,
        total_input_value,
        CASE
            WHEN output_count = 0 THEN 'malformed'
            WHEN input_count >= 2
                 AND cj_total_outputs >= 2
                 AND cj_max_equal_outputs >= 2
                 AND CAST(cj_max_equal_outputs AS DOUBLE) / NULLIF(CAST(cj_total_outputs AS DOUBLE), 0) >= 0.5
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
