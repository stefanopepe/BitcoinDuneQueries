-- ============================================================
-- Query: Lending Action Ledger - Unified Multi-Protocol (Base Query)
-- Description: Unified action ledger combining Aave V3 and Morpho Blue
--              lending events into a
--              single normalized schema.
--              Scoped to stablecoins (USDC, USDbC, USDT, DAI).
--              This is the primary base query for cross-protocol loop analysis.
--              Uses incremental processing with 1-day lookback.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-11
-- Architecture: V2 Base Query - computes ALL actions once across protocols
-- Dependencies: None (base query)
-- ============================================================
-- Protocols Included:
--   - aave_v3: Aave V3 direct lending (143k borrows/90d)
--   - morpho_blue: Morpho Blue isolated markets (30k borrows/90d)
--   - compound_v3: Optional (include when Base decoded tables are validated)
-- ============================================================
-- Asset Scope: Stablecoins only
--   - USDC   0x833589fcd6edb6e08f4c7c32d4f71b54bda02913
--   - USDbC  0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca
--   - USDT   0xfde4c96c8593536e31f229ea8f37b2ada2699bb2
--   - DAI    0x50c5725949a6f0c72e6c4a641f24049a917db0cb
-- ============================================================
-- Output Columns:
--   block_time           - Event timestamp
--   block_date           - Event date (for aggregation)
--   block_number         - Block number
--   tx_hash              - Transaction hash
--   evt_index            - Event log index
--   protocol             - Protocol identifier
--   action_type          - Action: supply/borrow/repay/withdraw/liquidation
--   user_address         - Entity performing action
--   on_behalf_of         - Beneficiary address (for entity resolution)
--   entity_address       - Canonical entity (COALESCE of on_behalf_of, user)
--   asset_address        - Underlying asset contract (resolved from wrapper)
--   asset_symbol         - Token symbol
--   amount_raw           - Raw amount in asset decimals
--   amount               - Decimal-adjusted amount
--   amount_usd           - USD value at event time
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
            entity_address VARBINARY,
            asset_address VARBINARY,
            asset_symbol VARCHAR,
            amount_raw VARCHAR,
            amount DOUBLE,
            amount_usd DOUBLE
        )
    ))
),

-- 2) Checkpoint: recompute from 1-day lookback
checkpoint AS (
    SELECT
        COALESCE(MAX(block_date), DATE '2024-01-01') - INTERVAL '1' DAY AS cutoff_date
    FROM prev
),

-- ============================================================
-- STABLECOIN AND WRAPPER TOKEN MAPPINGS
-- ============================================================

-- Stablecoin addresses, symbols, and decimals (eliminates tokens.erc20 JOIN)
stablecoin_metadata AS (
    SELECT address, symbol, decimals
    FROM (
        VALUES
            (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 'USDC',   6),
            (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca, 'USDbC',  6),
            (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2, 'USDT',   6),
            (0x50c5725949a6f0c72e6c4a641f24049a917db0cb, 'DAI',   18)
    ) AS t(address, symbol, decimals)
),

stablecoins AS (
    SELECT address FROM stablecoin_metadata
),

-- Optional wrapper -> underlying mappings (populate as Base coverage expands)
wrapper_to_underlying AS (
    SELECT wrapper, underlying
    FROM (
        VALUES
            (CAST(NULL AS VARBINARY), CAST(NULL AS VARBINARY))
    ) AS t(wrapper, underlying)
    WHERE wrapper IS NOT NULL
),

-- ============================================================
-- MORPHO BLUE EVENTS
-- Morpho Blue uses market IDs. We filter to stablecoin loan markets
-- by joining createmarket events where loanToken is a stablecoin.
-- The `assets` column contains the amount in loan token decimals.
-- ============================================================

-- Resolve Morpho Blue market IDs to stablecoin loan tokens
morpho_blue_stablecoin_markets AS (
    SELECT
        id AS market_id,
        from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) AS loan_token
    FROM morpho_blue_base.morphoblue_evt_createmarket
    WHERE from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) IN (
        SELECT address FROM stablecoins
    )
),

morpho_blue_supply AS (
    SELECT
        s.evt_block_time AS block_time,
        s.evt_block_date AS block_date,
        s.evt_block_number AS block_number,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'morpho_blue' AS protocol,
        'supply' AS action_type,
        s.caller AS user_address,
        s.onBehalf AS on_behalf_of,
        m.loan_token AS raw_asset_address,
        CAST(s.assets AS UINT256) AS amount_raw
    FROM morpho_blue_base.morphoblue_evt_supply s
    INNER JOIN morpho_blue_stablecoin_markets m ON m.market_id = s.id
    CROSS JOIN checkpoint c
    WHERE s.evt_block_date >= c.cutoff_date
      AND s.evt_block_date < CURRENT_DATE
),

morpho_blue_borrow AS (
    SELECT
        b.evt_block_time AS block_time,
        b.evt_block_date AS block_date,
        b.evt_block_number AS block_number,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'morpho_blue' AS protocol,
        'borrow' AS action_type,
        b.caller AS user_address,
        b.onBehalf AS on_behalf_of,
        m.loan_token AS raw_asset_address,
        CAST(b.assets AS UINT256) AS amount_raw
    FROM morpho_blue_base.morphoblue_evt_borrow b
    INNER JOIN morpho_blue_stablecoin_markets m ON m.market_id = b.id
    CROSS JOIN checkpoint c
    WHERE b.evt_block_date >= c.cutoff_date
      AND b.evt_block_date < CURRENT_DATE
),

morpho_blue_repay AS (
    SELECT
        r.evt_block_time AS block_time,
        r.evt_block_date AS block_date,
        r.evt_block_number AS block_number,
        r.evt_tx_hash AS tx_hash,
        r.evt_index,
        'morpho_blue' AS protocol,
        'repay' AS action_type,
        r.caller AS user_address,
        r.onBehalf AS on_behalf_of,
        m.loan_token AS raw_asset_address,
        CAST(r.assets AS UINT256) AS amount_raw
    FROM morpho_blue_base.morphoblue_evt_repay r
    INNER JOIN morpho_blue_stablecoin_markets m ON m.market_id = r.id
    CROSS JOIN checkpoint c
    WHERE r.evt_block_date >= c.cutoff_date
      AND r.evt_block_date < CURRENT_DATE
),

morpho_blue_withdraw AS (
    SELECT
        w.evt_block_time AS block_time,
        w.evt_block_date AS block_date,
        w.evt_block_number AS block_number,
        w.evt_tx_hash AS tx_hash,
        w.evt_index,
        'morpho_blue' AS protocol,
        'withdraw' AS action_type,
        w.caller AS user_address,
        w.onBehalf AS on_behalf_of,
        m.loan_token AS raw_asset_address,
        CAST(w.assets AS UINT256) AS amount_raw
    FROM morpho_blue_base.morphoblue_evt_withdraw w
    INNER JOIN morpho_blue_stablecoin_markets m ON m.market_id = w.id
    CROSS JOIN checkpoint c
    WHERE w.evt_block_date >= c.cutoff_date
      AND w.evt_block_date < CURRENT_DATE
),

morpho_blue_liquidation AS (
    SELECT
        l.evt_block_time AS block_time,
        l.evt_block_date AS block_date,
        l.evt_block_number AS block_number,
        l.evt_tx_hash AS tx_hash,
        l.evt_index,
        'morpho_blue' AS protocol,
        'liquidation' AS action_type,
        l.caller AS user_address,
        l.borrower AS on_behalf_of,
        m.loan_token AS raw_asset_address,
        CAST(l.repaidAssets AS UINT256) AS amount_raw
    FROM morpho_blue_base.morphoblue_evt_liquidate l
    INNER JOIN morpho_blue_stablecoin_markets m ON m.market_id = l.id
    CROSS JOIN checkpoint c
    WHERE l.evt_block_date >= c.cutoff_date
      AND l.evt_block_date < CURRENT_DATE
),

-- ============================================================
-- AAVE V3 EVENTS
-- Aave V3 uses `reserve` (underlying) and `amount` directly
-- ============================================================

aave_v3_supply AS (
    SELECT
        s.evt_block_time AS block_time,
        s.evt_block_date AS block_date,
        s.evt_block_number AS block_number,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'aave_v3' AS protocol,
        'supply' AS action_type,
        s."user" AS user_address,
        s.onBehalfOf AS on_behalf_of,
        s.reserve AS raw_asset_address,
        s.amount AS amount_raw
    FROM aave_v3_base.pool_evt_supply s
    CROSS JOIN checkpoint c
    WHERE s.evt_block_date >= c.cutoff_date
      AND s.evt_block_date < CURRENT_DATE
      AND s.reserve IN (SELECT address FROM stablecoins)
),

aave_v3_borrow AS (
    SELECT
        b.evt_block_time AS block_time,
        b.evt_block_date AS block_date,
        b.evt_block_number AS block_number,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'aave_v3' AS protocol,
        'borrow' AS action_type,
        b."user" AS user_address,
        b.onBehalfOf AS on_behalf_of,
        b.reserve AS raw_asset_address,
        b.amount AS amount_raw
    FROM aave_v3_base.pool_evt_borrow b
    CROSS JOIN checkpoint c
    WHERE b.evt_block_date >= c.cutoff_date
      AND b.evt_block_date < CURRENT_DATE
      AND b.reserve IN (SELECT address FROM stablecoins)
),

aave_v3_repay AS (
    SELECT
        r.evt_block_time AS block_time,
        r.evt_block_date AS block_date,
        r.evt_block_number AS block_number,
        r.evt_tx_hash AS tx_hash,
        r.evt_index,
        'aave_v3' AS protocol,
        'repay' AS action_type,
        r.repayer AS user_address,
        r."user" AS on_behalf_of,
        r.reserve AS raw_asset_address,
        r.amount AS amount_raw
    FROM aave_v3_base.pool_evt_repay r
    CROSS JOIN checkpoint c
    WHERE r.evt_block_date >= c.cutoff_date
      AND r.evt_block_date < CURRENT_DATE
      AND r.reserve IN (SELECT address FROM stablecoins)
),

aave_v3_withdraw AS (
    SELECT
        w.evt_block_time AS block_time,
        w.evt_block_date AS block_date,
        w.evt_block_number AS block_number,
        w.evt_tx_hash AS tx_hash,
        w.evt_index,
        'aave_v3' AS protocol,
        'withdraw' AS action_type,
        w."user" AS user_address,
        w."to" AS on_behalf_of,
        w.reserve AS raw_asset_address,
        w.amount AS amount_raw
    FROM aave_v3_base.pool_evt_withdraw w
    CROSS JOIN checkpoint c
    WHERE w.evt_block_date >= c.cutoff_date
      AND w.evt_block_date < CURRENT_DATE
      AND w.reserve IN (SELECT address FROM stablecoins)
),

aave_v3_liquidation AS (
    SELECT
        l.evt_block_time AS block_time,
        l.evt_block_date AS block_date,
        l.evt_block_number AS block_number,
        l.evt_tx_hash AS tx_hash,
        l.evt_index,
        'aave_v3' AS protocol,
        'liquidation' AS action_type,
        l.liquidator AS user_address,
        l."user" AS on_behalf_of,
        l.debtAsset AS raw_asset_address,
        l.debtToCover AS amount_raw
    FROM aave_v3_base.pool_evt_liquidationcall l
    CROSS JOIN checkpoint c
    WHERE l.evt_block_date >= c.cutoff_date
      AND l.evt_block_date < CURRENT_DATE
      AND l.debtAsset IN (SELECT address FROM stablecoins)
),

-- ============================================================
-- COMPOUND V3/V2 NOTE
-- Base framework is configured for Base-available core protocols.
-- Add Compound sections here once decoded Base schemas and mappings
-- are validated for the selected stablecoin set.
-- ============================================================

-- ============================================================
-- UNION ALL PROTOCOLS
-- ============================================================

all_events AS (
    -- Morpho Blue
    SELECT * FROM morpho_blue_supply
    UNION ALL SELECT * FROM morpho_blue_borrow
    UNION ALL SELECT * FROM morpho_blue_repay
    UNION ALL SELECT * FROM morpho_blue_withdraw
    UNION ALL SELECT * FROM morpho_blue_liquidation
    -- Aave V3
    UNION ALL SELECT * FROM aave_v3_supply
    UNION ALL SELECT * FROM aave_v3_borrow
    UNION ALL SELECT * FROM aave_v3_repay
    UNION ALL SELECT * FROM aave_v3_withdraw
    UNION ALL SELECT * FROM aave_v3_liquidation
    -- Compound protocols intentionally excluded until Base table validation.
),

-- ============================================================
-- RESOLVE WRAPPER TOKENS AND ENRICH WITH METADATA + PRICES
-- ============================================================

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
        COALESCE(e.on_behalf_of, e.user_address) AS entity_address,
        -- Resolve wrapper tokens (cToken/Comet) to underlying
        COALESCE(w.underlying, e.raw_asset_address) AS asset_address,
        sm.symbol AS asset_symbol,
        CAST(e.amount_raw AS VARCHAR) AS amount_raw,
        CAST(e.amount_raw AS DOUBLE) / POWER(10, sm.decimals) AS amount,
        CAST(e.amount_raw AS DOUBLE) / POWER(10, sm.decimals) * p.price AS amount_usd
    FROM all_events e
    LEFT JOIN wrapper_to_underlying w
        ON w.wrapper = e.raw_asset_address
    -- Hardcoded metadata: eliminates tokens.erc20 JOIN
    LEFT JOIN stablecoin_metadata sm
        ON sm.address = COALESCE(w.underlying, e.raw_asset_address)
    -- Time-bounded price JOIN: partition pruning on prices.usd
    LEFT JOIN prices.usd p
        ON p.contract_address = COALESCE(w.underlying, e.raw_asset_address)
        AND p.blockchain = 'base'
        AND p.minute = date_trunc('minute', e.block_time)
        AND p.minute >= (SELECT cutoff_date FROM checkpoint)
        AND p.minute < CURRENT_DATE
),

-- ============================================================
-- INCREMENTAL MERGE
-- ============================================================

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
