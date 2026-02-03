-- ============================================================
-- ⚠️ DEPRECATED: This query is superseded by the V2 unified architecture.
-- Use bitcoin_cohort_distribution_v2.sql instead, which references
-- the unified base query bitcoin_tx_features_daily.sql.
-- This query remains for backward compatibility only.
-- ============================================================
-- Query: Bitcoin Cohort Distribution
-- Description: Classifies Bitcoin transactions by total input value
--              into holder cohorts (Shrimps through Humpback) based
--              on BTC volume moved. Tracks daily distribution of
--              transaction activity across different holder sizes.
--              Uses incremental processing with 1-day lookback.
-- Author: stefanopepe
-- Created: 2026-01-30
-- Updated: 2026-02-02 (deprecated)
-- Note: On first run, only processes data from fallback date onwards.
--       Adjust DATE '2026-01-01' in checkpoint CTE for historical analysis.
-- ============================================================
-- Cohort Definitions (by total tx input value in BTC):
--   Shrimps    - < 1 BTC
--   Crab       - 1-10 BTC
--   Octopus    - 10-50 BTC
--   Fish       - 50-100 BTC
--   Dolphin    - 100-500 BTC
--   Shark      - 500-1,000 BTC
--   Whale      - 1,000-5,000 BTC
--   Humpback   - > 5,000 BTC
-- ============================================================
-- Output Columns:
--   day              - Date of transactions
--   cohort           - Holder cohort name
--   cohort_order     - Numeric ordering for cohorts (1-8)
--   btc_moved        - Total BTC moved (input value)
--   tx_count         - Number of transactions
--   spent_utxo_count - Total UTXOs consumed
-- ============================================================

WITH
-- 1) Previous results (empty on first ever run)
prev AS (
    SELECT *
    FROM TABLE(previous.query.result(
        schema => DESCRIPTOR(
            day DATE,
            cohort VARCHAR,
            cohort_order BIGINT,
            btc_moved DOUBLE,
            tx_count BIGINT,
            spent_utxo_count BIGINT
        )
    ))
),

-- 2) Checkpoint: recompute from 1-day lookback
checkpoint AS (
    SELECT
        COALESCE(MAX(day), DATE '2026-01-01') - INTERVAL '1' DAY AS cutoff_day
    FROM prev
),

-- 3) Count spent UTXOs per transaction (non-coinbase inputs only)
spent_utxos_per_tx AS (
    SELECT
        CAST(date_trunc('day', i.block_time) AS DATE) AS day,
        i.tx_id,
        COUNT(*) AS spent_utxo_count
    FROM bitcoin.inputs i
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', i.block_time) AS DATE) >= c.cutoff_day
      AND CAST(date_trunc('day', i.block_time) AS DATE) < CURRENT_DATE
      AND i.is_coinbase = FALSE
    GROUP BY 1, 2
),

-- 4) Get transaction-level input totals from bitcoin.transactions
-- Note: input_value is already in BTC (not satoshis) in Dune's bitcoin.transactions
tx_input_totals AS (
    SELECT
        CAST(date_trunc('day', t.block_time) AS DATE) AS day,
        t.id AS tx_id,
        t.input_value AS input_value_btc
    FROM bitcoin.transactions t
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', t.block_time) AS DATE) >= c.cutoff_day
      AND CAST(date_trunc('day', t.block_time) AS DATE) < CURRENT_DATE
      AND t.input_count > 0
),

-- 5) Classify transactions by cohort based on input value (in BTC)
tx_cohorts AS (
    SELECT
        t.day,
        t.tx_id,
        t.input_value_btc,
        CASE
            WHEN t.input_value_btc < 1 THEN 'Shrimps (<1 BTC)'
            WHEN t.input_value_btc < 10 THEN 'Crab (1-10 BTC)'
            WHEN t.input_value_btc < 50 THEN 'Octopus (10-50 BTC)'
            WHEN t.input_value_btc < 100 THEN 'Fish (50-100 BTC)'
            WHEN t.input_value_btc < 500 THEN 'Dolphin (100-500 BTC)'
            WHEN t.input_value_btc < 1000 THEN 'Shark (500-1,000 BTC)'
            WHEN t.input_value_btc < 5000 THEN 'Whale (1,000-5,000 BTC)'
            ELSE 'Humpback (>5,000 BTC)'
        END AS cohort,
        CASE
            WHEN t.input_value_btc < 1 THEN 1
            WHEN t.input_value_btc < 10 THEN 2
            WHEN t.input_value_btc < 50 THEN 3
            WHEN t.input_value_btc < 100 THEN 4
            WHEN t.input_value_btc < 500 THEN 5
            WHEN t.input_value_btc < 1000 THEN 6
            WHEN t.input_value_btc < 5000 THEN 7
            ELSE 8
        END AS cohort_order
    FROM tx_input_totals t
),

-- 6) Aggregate by day and cohort
new_data AS (
    SELECT
        tc.day,
        tc.cohort,
        tc.cohort_order,
        SUM(tc.input_value_btc) AS btc_moved,
        COUNT(*) AS tx_count,
        SUM(COALESCE(s.spent_utxo_count, 0)) AS spent_utxo_count
    FROM tx_cohorts tc
    LEFT JOIN spent_utxos_per_tx s
        ON s.day = tc.day
        AND s.tx_id = tc.tx_id
    GROUP BY 1, 2, 3
),

-- 7) Keep historical data before cutoff
kept_old AS (
    SELECT p.*
    FROM prev p
    CROSS JOIN checkpoint c
    WHERE p.day < c.cutoff_day
)

-- 8) Final combined result
SELECT * FROM kept_old
UNION ALL
SELECT * FROM new_data
ORDER BY day, cohort_order;
