-- ============================================================
-- Diagnostic: Check bitcoin.transactions.input_value units
-- Description: Determine if input_value is in satoshis or BTC
-- Usage: Copy/paste to Dune and run. Check the output values.
-- ============================================================

WITH
block_cutoff AS (
    SELECT MAX(height) - 10 AS min_height FROM bitcoin.blocks
),

sample_txs AS (
    SELECT
        t.id AS tx_id,
        t.input_value,
        t.input_count,
        t.output_value,
        t.fee
    FROM bitcoin.transactions t
    CROSS JOIN block_cutoff bc
    WHERE t.block_height >= bc.min_height
      AND t.input_count > 0
    LIMIT 20
)

SELECT
    tx_id,
    input_value AS input_value_raw,
    input_value / 1e8 AS input_value_if_sats_to_btc,
    input_value * 1e8 AS input_value_if_btc_to_sats,
    input_count,
    output_value AS output_value_raw,
    fee AS fee_raw,
    -- Sanity check: if input_value is in satoshis, this should be reasonable BTC amounts
    CASE
        WHEN input_value / 1e8 BETWEEN 0.0001 AND 10000 THEN 'likely_satoshis'
        WHEN input_value BETWEEN 0.0001 AND 10000 THEN 'likely_btc'
        ELSE 'unclear'
    END AS unit_guess
FROM sample_txs
ORDER BY input_value DESC;
