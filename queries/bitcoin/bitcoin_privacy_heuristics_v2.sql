-- ============================================================
-- ⚠️ DEPRECATED: This query is superseded by the V3 unified architecture.
-- Use bitcoin_privacy_heuristics_v3.sql for basic privacy analysis, which
-- references the unified base query bitcoin_tx_features_daily.sql.
-- This query remains for advanced analysis (change_precision, UIH1, UIH2).
-- ============================================================
-- Query: Bitcoin Privacy Heuristics V2
-- Description: Implements advanced privacy analysis heuristics based on
--              Blockstream Esplora's privacy-analysis.js methodology.
--              Analyzes ONLY transactions classified as "other" by the
--              UTXO Heuristics query (query_6614095).
--              Detects: change outputs (precision/script mismatch), UIH,
--              and address reuse patterns.
-- Author: stefanopepe
-- Created: 2026-01-29
-- Updated: 2026-02-02 (deprecated)
-- Reference: https://github.com/Blockstream/esplora/blob/master/client/src/lib/privacy-analysis.js
-- Dependency: Runs on "other" intent from query_6614095 (Bitcoin UTXO Heuristics)
-- Note: Uses incremental processing with 1-day lookback.
-- ============================================================
-- Privacy Heuristics Implemented:
--   1. change_precision    - Change detected via decimal precision difference (≥3 digits)
--   2. change_script_type  - Change detected via script type mismatch
--   3. uih1                - Unnecessary Input Heuristic 1 (smallest input covers smallest output)
--   4. uih2                - Unnecessary Input Heuristic 2 (smallest input covers largest output)
--   5. address_reuse       - Output script matches an input script
--   6. no_privacy_issues   - No heuristics triggered
-- Note: coinjoin_detected and self_transfer are handled by UTXO Heuristics layer
-- ============================================================
-- Output Columns:
--   day                   - Date of transactions
--   privacy_heuristic     - The privacy issue detected
--   tx_count              - Number of transactions
--   sats_total            - Total satoshis involved
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
            sats_total DOUBLE
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

-- 4) Get all spendable outputs (exclude OP_RETURN and other non-spendable types)
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
      AND o.type NOT IN ('nulldata', 'nonstandard')  -- Exclude OP_RETURN and non-spendable
),

-- 5) Get tx-level input/output counts for UTXO classification
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

-- 6) Filter to "other" intent (exclude all UTXO-classified categories)
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

-- 7) Aggregate transaction-level input stats (filtered to "other" only)
tx_input_stats AS (
    SELECT
        ri.day,
        ri.tx_id,
        COUNT(*) AS input_count,
        SUM(ri.input_value) AS total_input_value,
        MIN(ri.input_value) AS min_input_value,
        MAX(ri.input_value) AS max_input_value,
        -- Collect distinct input script types
        ARRAY_AGG(DISTINCT ri.input_script_type) AS input_script_types,
        -- Check if all inputs have same script type (for UIH exclusion)
        COUNT(DISTINCT ri.input_script_type) AS distinct_input_script_count
    FROM raw_inputs ri
    INNER JOIN other_tx_ids o ON ri.day = o.day AND ri.tx_id = o.tx_id
    GROUP BY ri.day, ri.tx_id
),

-- 8) Aggregate transaction-level output stats (filtered to "other" only)
tx_output_stats AS (
    SELECT
        ro.day,
        ro.tx_id,
        COUNT(*) AS output_count,
        SUM(ro.output_value) AS total_output_value,
        MIN(ro.output_value) AS min_output_value,
        MAX(ro.output_value) AS max_output_value,
        -- Collect distinct output script types
        ARRAY_AGG(DISTINCT ro.output_script_type) AS output_script_types,
        COUNT(DISTINCT ro.output_script_type) AS distinct_output_script_count
    FROM raw_outputs ro
    INNER JOIN other_tx_ids o ON ro.day = o.day AND ro.tx_id = o.tx_id
    GROUP BY ro.day, ro.tx_id
),

-- 9) For 2-output transactions, get individual output details for precision analysis
two_output_details AS (
    SELECT
        o.day,
        o.tx_id,
        MIN(CASE WHEN rn = 1 THEN output_value END) AS out1_value,
        MIN(CASE WHEN rn = 2 THEN output_value END) AS out2_value,
        MIN(CASE WHEN rn = 1 THEN output_script_type END) AS out1_type,
        MIN(CASE WHEN rn = 2 THEN output_script_type END) AS out2_type
    FROM (
        SELECT
            day,
            tx_id,
            output_value,
            output_script_type,
            ROW_NUMBER() OVER (PARTITION BY day, tx_id ORDER BY output_index) AS rn
        FROM raw_outputs
        WHERE tx_id IN (
            SELECT tx_id
            FROM tx_output_stats
            WHERE output_count = 2
        )
    ) o
    GROUP BY o.day, o.tx_id
),

-- Note: CoinJoin detection removed - handled by UTXO Heuristics (coinjoin_like)

-- 10) Detect address reuse: output address matches input address (filtered to "other" only)
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

-- 11) Combine all data for classification
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
        tod.out1_value,
        tod.out2_value,
        tod.out1_type,
        tod.out2_type,
        -- CoinJoin metrics
        COALESCE(cj.total_outputs, 0) AS cj_total_outputs,
        COALESCE(cj.max_equal_outputs, 0) AS cj_max_equal_outputs,
        -- Address reuse
        COALESCE(ar.has_address_reuse, FALSE) AS has_address_reuse
    FROM tx_input_stats i
    LEFT JOIN tx_output_stats o ON i.day = o.day AND i.tx_id = o.tx_id
    LEFT JOIN two_output_details tod ON i.day = tod.day AND i.tx_id = tod.tx_id
    LEFT JOIN address_reuse_detection ar ON i.day = ar.day AND i.tx_id = ar.tx_id
),

-- 11) Calculate precision for 2-output transactions
-- Precision = number of trailing zeros when expressed in satoshis (using modulo)
tx_with_precision AS (
    SELECT
        *,
        -- For 2-output txs, calculate precision difference using modulo
        CASE
            WHEN output_count = 2 AND out1_value IS NOT NULL AND out2_value IS NOT NULL
                 AND out1_value > 0 AND out2_value > 0 THEN
                ABS(
                    -- Count trailing zeros for first output
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
                    -- Count trailing zeros for second output
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
        -- Check if output script types differ
        CASE
            WHEN output_count = 2 AND out1_type IS NOT NULL AND out2_type IS NOT NULL
                 AND out1_type != out2_type THEN TRUE
            ELSE FALSE
        END AS script_types_differ
    FROM tx_combined
),

-- 13) Apply all privacy heuristics
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

            -- Note: coinjoin_detected removed - handled by UTXO Heuristics (coinjoin_like)
            -- Note: self_transfer removed - handled by UTXO Heuristics (self_transfer)

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

-- 14) Aggregate by day and heuristic
new_data AS (
    SELECT
        day,
        privacy_heuristic,
        COUNT(*) AS tx_count,
        SUM(total_input_value) AS sats_total
    FROM classified
    GROUP BY day, privacy_heuristic
),

-- 15) Keep historical data before cutoff
kept_old AS (
    SELECT p.*
    FROM prev p
    CROSS JOIN checkpoint c
    WHERE p.day < c.cutoff_day
)

-- 16) Final combined result
SELECT * FROM kept_old
UNION ALL
SELECT * FROM new_data
ORDER BY day, privacy_heuristic;
