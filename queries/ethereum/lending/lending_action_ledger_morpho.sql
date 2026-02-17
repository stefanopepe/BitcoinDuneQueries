-- ============================================================
-- Query: Lending Action Ledger - Morpho Aave V2 (Base Query)
-- Description: Unified action ledger for all Morpho Aave V2 lending events.
--              Fetches Supply, Borrow, Repay, Withdraw, Liquidation events
--              and normalizes them into a single schema for downstream analysis.
--              Uses incremental processing with 1-day lookback.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-17
-- Architecture: V2 Base Query - computes ALL actions once
-- Dependencies: None (base query)
-- ============================================================
-- Output Columns:
--   block_time           - Event timestamp
--   block_date           - Event date (for aggregation)
--   block_number         - Block number
--   tx_hash              - Transaction hash
--   evt_index            - Event log index
--   protocol             - Protocol identifier ('morpho_aave_v2')
--   action_type          - Action: supply/borrow/repay/withdraw/liquidation
--   user_address         - Entity performing action
--   on_behalf_of         - Beneficiary address (for entity resolution)
--   asset_address        - Underlying asset contract
--   pool_token           - Aave aToken address (Morpho-specific)
--   amount_raw           - Raw amount in asset decimals
--   amount               - Decimal-adjusted amount
--   amount_usd           - USD value at event time
--   balance_p2p          - Morpho P2P matched balance
--   balance_pool         - Morpho Aave pool balance
-- ============================================================

WITH
-- 1) Previous results (empty on first run)
prev AS (
    SELECT *
    FROM TABLE(previous.query.result(
        schema => DESCRIPTOR(
            block_time TIMESTAMP,
            block_date DATE,
            block_number BIGINT,
            tx_hash VARBINARY,
            evt_index BIGINT,
            protocol VARCHAR,
            action_type VARCHAR,
            user_address VARBINARY,
            on_behalf_of VARBINARY,
            asset_address VARBINARY,
            pool_token VARBINARY,
            amount_raw VARCHAR,
            amount DOUBLE,
            amount_usd DOUBLE,
            balance_p2p VARCHAR,
            balance_pool VARCHAR
        )
    ))
),

-- 2) Checkpoint: recompute from 1-day lookback
checkpoint AS (
    SELECT
        COALESCE(MAX(block_date), DATE '2024-01-01') - INTERVAL '1' DAY AS cutoff_date
    FROM prev
),

-- 3) Supply events
supply_events AS (
    SELECT
        s.evt_block_time AS block_time,
        CAST(date_trunc('day', s.evt_block_time) AS DATE) AS block_date,
        s.evt_block_number AS block_number,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'morpho_aave_v2' AS protocol,
        'supply' AS action_type,
        s._from AS user_address,
        s._onBehalf AS on_behalf_of,
        CAST(NULL AS VARBINARY) AS asset_address,  -- Will be enriched later
        s._poolToken AS pool_token,
        s._amount AS amount_raw,
        CAST(s._balanceInP2P AS VARCHAR) AS balance_p2p,
        CAST(s._balanceOnPool AS VARCHAR) AS balance_pool
    FROM morpho_aave_v2_ethereum.morpho_evt_supplied s
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', s.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', s.evt_block_time) AS DATE) < CURRENT_DATE
),

-- 4) Borrow events
borrow_events AS (
    SELECT
        b.evt_block_time AS block_time,
        CAST(date_trunc('day', b.evt_block_time) AS DATE) AS block_date,
        b.evt_block_number AS block_number,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'morpho_aave_v2' AS protocol,
        'borrow' AS action_type,
        b._borrower AS user_address,
        b._borrower AS on_behalf_of,  -- Morpho borrow doesn't have onBehalf
        CAST(NULL AS VARBINARY) AS asset_address,
        b._poolToken AS pool_token,
        b._amount AS amount_raw,
        CAST(b._balanceInP2P AS VARCHAR) AS balance_p2p,
        CAST(b._balanceOnPool AS VARCHAR) AS balance_pool
    FROM morpho_aave_v2_ethereum.morpho_evt_borrowed b
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', b.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', b.evt_block_time) AS DATE) < CURRENT_DATE
),

-- 5) Repay events
repay_events AS (
    SELECT
        r.evt_block_time AS block_time,
        CAST(date_trunc('day', r.evt_block_time) AS DATE) AS block_date,
        r.evt_block_number AS block_number,
        r.evt_tx_hash AS tx_hash,
        r.evt_index,
        'morpho_aave_v2' AS protocol,
        'repay' AS action_type,
        r._repayer AS user_address,
        r._onBehalf AS on_behalf_of,
        CAST(NULL AS VARBINARY) AS asset_address,
        r._poolToken AS pool_token,
        r._amount AS amount_raw,
        CAST(r._balanceInP2P AS VARCHAR) AS balance_p2p,
        CAST(r._balanceOnPool AS VARCHAR) AS balance_pool
    FROM morpho_aave_v2_ethereum.morpho_evt_repaid r
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', r.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', r.evt_block_time) AS DATE) < CURRENT_DATE
),

-- 6) Withdraw events
withdraw_events AS (
    SELECT
        w.evt_block_time AS block_time,
        CAST(date_trunc('day', w.evt_block_time) AS DATE) AS block_date,
        w.evt_block_number AS block_number,
        w.evt_tx_hash AS tx_hash,
        w.evt_index,
        'morpho_aave_v2' AS protocol,
        'withdraw' AS action_type,
        w._supplier AS user_address,
        w._receiver AS on_behalf_of,
        CAST(NULL AS VARBINARY) AS asset_address,
        w._poolToken AS pool_token,
        w._amount AS amount_raw,
        CAST(w._balanceInP2P AS VARCHAR) AS balance_p2p,
        CAST(w._balanceOnPool AS VARCHAR) AS balance_pool
    FROM morpho_aave_v2_ethereum.morpho_evt_withdrawn w
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', w.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', w.evt_block_time) AS DATE) < CURRENT_DATE
),

-- 7) Liquidation events
liquidation_events AS (
    SELECT
        l.evt_block_time AS block_time,
        CAST(date_trunc('day', l.evt_block_time) AS DATE) AS block_date,
        l.evt_block_number AS block_number,
        l.evt_tx_hash AS tx_hash,
        l.evt_index,
        'morpho_aave_v2' AS protocol,
        'liquidation' AS action_type,
        l._liquidated AS user_address,
        l._liquidator AS on_behalf_of,
        CAST(NULL AS VARBINARY) AS asset_address,
        l._poolTokenBorrowed AS pool_token,  -- Debt asset
        l._amountRepaid AS amount_raw,
        CAST('0' AS VARCHAR) AS balance_p2p,
        CAST('0' AS VARCHAR) AS balance_pool
    FROM morpho_aave_v2_ethereum.morpho_evt_liquidated l
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', l.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', l.evt_block_time) AS DATE) < CURRENT_DATE
),

-- 8) Union all events
all_events AS (
    SELECT * FROM supply_events
    UNION ALL
    SELECT * FROM borrow_events
    UNION ALL
    SELECT * FROM repay_events
    UNION ALL
    SELECT * FROM withdraw_events
    UNION ALL
    SELECT * FROM liquidation_events
),

-- 9) Enrich with token metadata and prices
-- Note: Morpho uses Aave aToken addresses. We need to map to underlying.
-- For simplicity, we use a direct price lookup on the pool_token.
-- In production, you'd join to an aToken->underlying mapping table.
enriched AS (
    SELECT
        e.block_time,
        e.block_date,
        e.block_number,
        e.tx_hash,
        e.evt_index,
        e.protocol,
        e.action_type,
        e.user_address,
        e.on_behalf_of,
        e.asset_address,
        e.pool_token,
        CAST(e.amount_raw AS VARCHAR) AS amount_raw,
        -- For now, assume 18 decimals (most common)
        -- In production, join to token metadata
        CAST(e.amount_raw AS DOUBLE) / 1e18 AS amount,
        -- USD value placeholder (would join to prices.usd in production)
        CAST(NULL AS DOUBLE) AS amount_usd,
        CAST(e.balance_p2p AS VARCHAR) AS balance_p2p,
        CAST(e.balance_pool AS VARCHAR) AS balance_pool
    FROM all_events e
),

-- 10) Incremental merge: keep old data before cutoff, add new data
new_data AS (
    SELECT * FROM enriched
),

kept_old AS (
    SELECT p.*
    FROM prev p
    CROSS JOIN checkpoint c
    WHERE p.block_date < c.cutoff_date
)

SELECT * FROM kept_old
UNION ALL
SELECT * FROM new_data
ORDER BY block_date, block_time, tx_hash, evt_index
