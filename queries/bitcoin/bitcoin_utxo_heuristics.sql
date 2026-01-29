-- ============================================================
-- Query: Bitcoin UTXO Heuristics
-- Description: Classifies Bitcoin transactions by intent using
--              input/output patterns (consolidation, fan-out batch,
--              coinjoin-like, self-transfer, change-like, other).
--              Uses incremental processing with 1-day lookback.
-- Author: stefanopepe
-- Created: 2026-01-28
-- Updated: 2026-01-29
-- Dune Link: https://dune.com/queries/6614095/
-- Note: On first run, only processes data from fallback date onwards.
--       Adjust DATE '2026-01-01' in checkpoint CTE for historical analysis.
-- ============================================================
-- Output Columns:
--   day            - Date of transactions
--   intent         - Classified transaction intent
--   tx_count       - Number of transactions
--   sats_in        - Total input value (satoshis)
--   sats_out       - Total output value (satoshis)
--   avg_inputs     - Average input count per tx
--   avg_outputs    - Average output count per tx
--   median_inputs  - Median input count per tx
--   median_outputs - Median output count per tx
-- ============================================================

WITH
-- 1) Previous results (empty on first ever run)
prev AS (
    SELECT *
    FROM TABLE(previous.query.result(
        schema => DESCRIPTOR(
            day DATE,
            intent VARCHAR,
            tx_count BIGINT,
            sats_in DOUBLE,
            sats_out DOUBLE,
            avg_inputs DOUBLE,
            avg_outputs DOUBLE,
            median_inputs DOUBLE,
            median_outputs DOUBLE
        )
    ))
),

-- 2) Recompute tail window (1-day lookback from last stored day)
checkpoint AS (
    SELECT
        COALESCE(MAX(day), DATE '2026-01-01') - INTERVAL '1' DAY AS cutoff_day
    FROM prev
),

-- 3) Build tx-level features for days in [cutoff_day, current_date)
inputs_by_tx AS (
    SELECT
        CAST(date_trunc('day', i.block_time) AS DATE) AS day,
        i.tx_id                                       AS tx_id,
        COUNT(*)                                      AS input_count,
        SUM(i.value)                                  AS input_value_sats
    FROM bitcoin.inputs i
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', i.block_time) AS DATE) >= c.cutoff_day
      AND CAST(date_trunc('day', i.block_time) AS DATE) <  CURRENT_DATE
      AND i.is_coinbase = FALSE
    GROUP BY 1, 2
),

outputs_by_tx AS (
    SELECT
        CAST(date_trunc('day', o.block_time) AS DATE) AS day,
        o.tx_id                                       AS tx_id,
        COUNT(*)                                      AS output_count,
        SUM(o.value)                                  AS output_value_sats
    FROM bitcoin.outputs o
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', o.block_time) AS DATE) >= c.cutoff_day
      AND CAST(date_trunc('day', o.block_time) AS DATE) <  CURRENT_DATE
    GROUP BY 1, 2
),

tx AS (
    -- spending tx universe: every row here is a tx with >=1 non-coinbase input
    SELECT
        i.day,
        i.tx_id,
        i.input_count,
        i.input_value_sats,
        COALESCE(o.output_count, 0)              AS output_count,
        COALESCE(o.output_value_sats, 0)         AS output_value_sats
    FROM inputs_by_tx i
    LEFT JOIN outputs_by_tx o
      ON  i.day = o.day
      AND i.tx_id = o.tx_id
),

classified AS (
    SELECT
        day,
        tx_id,
        input_count,
        output_count,
        input_value_sats,
        output_value_sats,
        CASE
            -- Guard: skip malformed tx with no outputs
            WHEN output_count = 0
                THEN 'malformed_no_outputs'
            WHEN input_count >= 10 AND output_count <= 2
                THEN 'consolidation'
            WHEN input_count <= 2 AND output_count >= 10
                THEN 'fan_out_batch'
            WHEN input_count >= 5 AND output_count >= 5 AND abs(input_count - output_count) <= 1
                THEN 'coinjoin_like'
            WHEN input_count = 1 AND output_count = 1
                THEN 'self_transfer'
            WHEN output_count = 2 AND input_count >= 2
                THEN 'change_like_2_outputs'
            ELSE 'other'
        END AS intent
    FROM tx
),

new_data AS (
    SELECT
        day                                                AS day,
        intent                                             AS intent,
        COUNT(*)                                           AS tx_count,
        SUM(input_value_sats)                              AS sats_in,
        SUM(output_value_sats)                             AS sats_out,
        AVG(input_count)                                   AS avg_inputs,
        AVG(output_count)                                  AS avg_outputs,
        APPROX_PERCENTILE(input_count, 0.5)                AS median_inputs,
        APPROX_PERCENTILE(output_count, 0.5)               AS median_outputs
    FROM classified
    GROUP BY 1, 2
),

-- 4) Keep historical rows strictly before cutoff_day
kept_old AS (
    SELECT p.*
    FROM prev p
    CROSS JOIN checkpoint c
    WHERE p.day < c.cutoff_day
)

-- 5) Final accrued result
SELECT * FROM kept_old
UNION ALL
SELECT * FROM new_data
ORDER BY day, intent;
