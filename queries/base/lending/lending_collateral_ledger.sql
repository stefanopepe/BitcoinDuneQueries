-- ============================================================
-- Query: Lending Collateral Ledger (Base Query)
-- Description: Tracks collateral deposits and withdrawals across
--              Aave V3 and Morpho Blue.
--              Captures the non-stablecoin side of lending positions
--              (cbBTC, WBTC, WETH, eth_lst) to answer:
--              "What backs the stablecoin borrows?"
--              Uses incremental processing with 1-day lookback.
-- Author: stefanopepe
-- Created: 2026-02-12
-- Updated: 2026-02-12
-- Architecture: V2 Base Query - parallel to lending_action_ledger_unified
-- Dependencies: None (base query)
-- ============================================================
-- Protocols Included:
--   - aave_v3: Supply/Withdraw events where reserve is NOT a stablecoin
--   - morpho_blue: SupplyCollateral/WithdrawCollateral events
--   - compound_v3/v2: intentionally omitted until Base decoded coverage
--                     and mappings are validated
-- ============================================================
-- Collateral Assets Tracked:
--   BTC:     cbBTC  0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf (8 decimals)
--   BTC:     WBTC   0x0555e30da8f98308edb960aa94c0db47230d2b9c (8 decimals)
--   ETH:     WETH   0x4200000000000000000000000000000000000006 (18 decimals)
--   ETH LST: wstETH, weETH, rETH, cbETH (resolved from tokens.erc20 on Base)
-- ============================================================
-- Output Columns:
--   block_time           - Event timestamp
--   block_date           - Event date (for aggregation)
--   block_number         - Block number
--   tx_hash              - Transaction hash
--   evt_index            - Event log index
--   protocol             - Protocol identifier
--   action_type          - supply_collateral / withdraw_collateral
--   user_address         - Entity performing action
--   on_behalf_of         - Beneficiary address
--   entity_address       - Canonical entity (COALESCE of on_behalf_of, user)
--   collateral_address   - Underlying collateral asset contract
--   collateral_symbol    - Token symbol (WBTC, WETH, wstETH, etc.)
--   collateral_category  - btc / eth / eth_lst
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
            collateral_address VARBINARY,
            collateral_symbol VARCHAR,
            collateral_category VARCHAR,
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
-- COLLATERAL ASSET METADATA (hardcoded — no tokens.erc20 JOIN)
-- ============================================================

core_collateral_metadata AS (
    SELECT address, symbol, decimals, category
    FROM (
        VALUES
            (0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf, 'cbBTC',  8, 'btc'),
            (0x0555e30da8f98308edb960aa94c0db47230d2b9c, 'WBTC',   8, 'btc'),
            (0x4200000000000000000000000000000000000006, 'WETH',  18, 'eth')
    ) AS t(address, symbol, decimals, category)
),

eth_lst_collateral_metadata AS (
    SELECT
        t.contract_address AS address,
        t.symbol,
        t.decimals,
        'eth_lst' AS category
    FROM tokens.erc20 t
    WHERE t.blockchain = 'base'
      AND t.symbol IN ('wstETH', 'weETH', 'rETH', 'cbETH')
),

collateral_metadata AS (
    SELECT * FROM core_collateral_metadata
    UNION ALL
    SELECT * FROM eth_lst_collateral_metadata
),

collateral_addresses AS (
    SELECT address FROM collateral_metadata
),

-- Stablecoin addresses (to EXCLUDE from Aave V3 supply/withdraw)
stablecoin_addresses AS (
    SELECT address
    FROM (
        VALUES
            (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913),  -- USDC
            (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca),  -- USDbC
            (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2),  -- USDT
            (0x50c5725949a6f0c72e6c4a641f24049a917db0cb)   -- DAI
    ) AS t(address)
),

-- ============================================================
-- MORPHO BLUE COLLATERAL EVENTS
-- Morpho Blue has explicit SupplyCollateral/WithdrawCollateral events.
-- We resolve collateral token from createmarket's marketParams.
-- ============================================================

-- Resolve Morpho Blue market IDs to collateral tokens
morpho_blue_collateral_markets AS (
    SELECT
        id AS market_id,
        from_hex(substr(json_extract_scalar(marketParams, '$.collateralToken'), 3)) AS collateral_token
    FROM morpho_blue_base.morphoblue_evt_createmarket
    WHERE from_hex(substr(json_extract_scalar(marketParams, '$.collateralToken'), 3)) IN (
        SELECT address FROM collateral_addresses
    )
),

morpho_blue_supply_collateral AS (
    SELECT
        s.evt_block_time AS block_time,
        s.evt_block_date AS block_date,
        s.evt_block_number AS block_number,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'morpho_blue' AS protocol,
        'supply_collateral' AS action_type,
        s.caller AS user_address,
        s.onBehalf AS on_behalf_of,
        m.collateral_token AS raw_asset_address,
        CAST(s.assets AS UINT256) AS amount_raw
    FROM morpho_blue_base.morphoblue_evt_supplycollateral s
    INNER JOIN morpho_blue_collateral_markets m ON m.market_id = s.id
    CROSS JOIN checkpoint c
    WHERE s.evt_block_date >= c.cutoff_date
      AND s.evt_block_date < CURRENT_DATE
),

morpho_blue_withdraw_collateral AS (
    SELECT
        w.evt_block_time AS block_time,
        w.evt_block_date AS block_date,
        w.evt_block_number AS block_number,
        w.evt_tx_hash AS tx_hash,
        w.evt_index,
        'morpho_blue' AS protocol,
        'withdraw_collateral' AS action_type,
        w.caller AS user_address,
        w.onBehalf AS on_behalf_of,
        m.collateral_token AS raw_asset_address,
        CAST(w.assets AS UINT256) AS amount_raw
    FROM morpho_blue_base.morphoblue_evt_withdrawcollateral w
    INNER JOIN morpho_blue_collateral_markets m ON m.market_id = w.id
    CROSS JOIN checkpoint c
    WHERE w.evt_block_date >= c.cutoff_date
      AND w.evt_block_date < CURRENT_DATE
),

-- ============================================================
-- AAVE V3 COLLATERAL EVENTS
-- Aave V3 uses the same Supply/Withdraw events for all assets.
-- We capture supply/withdraw where reserve is a tracked collateral
-- asset (NOT a stablecoin).
-- ============================================================

aave_v3_supply_collateral AS (
    SELECT
        s.evt_block_time AS block_time,
        s.evt_block_date AS block_date,
        s.evt_block_number AS block_number,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'aave_v3' AS protocol,
        'supply_collateral' AS action_type,
        s."user" AS user_address,
        s.onBehalfOf AS on_behalf_of,
        s.reserve AS raw_asset_address,
        s.amount AS amount_raw
    FROM aave_v3_base.pool_evt_supply s
    CROSS JOIN checkpoint c
    WHERE s.evt_block_date >= c.cutoff_date
      AND s.evt_block_date < CURRENT_DATE
      AND s.reserve IN (SELECT address FROM collateral_addresses)
),

aave_v3_withdraw_collateral AS (
    SELECT
        w.evt_block_time AS block_time,
        w.evt_block_date AS block_date,
        w.evt_block_number AS block_number,
        w.evt_tx_hash AS tx_hash,
        w.evt_index,
        'aave_v3' AS protocol,
        'withdraw_collateral' AS action_type,
        w."user" AS user_address,
        w."to" AS on_behalf_of,
        w.reserve AS raw_asset_address,
        w.amount AS amount_raw
    FROM aave_v3_base.pool_evt_withdraw w
    CROSS JOIN checkpoint c
    WHERE w.evt_block_date >= c.cutoff_date
      AND w.evt_block_date < CURRENT_DATE
      AND w.reserve IN (SELECT address FROM collateral_addresses)
),

-- Compound sections intentionally omitted until Base decoded coverage
-- and contract mappings are explicitly validated.

-- ============================================================
-- UNION ALL PROTOCOLS
-- ============================================================

all_events AS (
    -- Morpho Blue
    SELECT * FROM morpho_blue_supply_collateral
    UNION ALL SELECT * FROM morpho_blue_withdraw_collateral
    -- Aave V3
    UNION ALL SELECT * FROM aave_v3_supply_collateral
    UNION ALL SELECT * FROM aave_v3_withdraw_collateral
    -- Compound protocols intentionally omitted for Base.
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
        e.raw_asset_address AS collateral_address,
        cm.symbol AS collateral_symbol,
        cm.category AS collateral_category,
        CAST(e.amount_raw AS VARCHAR) AS amount_raw,
        CAST(e.amount_raw AS DOUBLE) / POWER(10, cm.decimals) AS amount,
        CAST(e.amount_raw AS DOUBLE) / POWER(10, cm.decimals) * p.price AS amount_usd
    FROM all_events e
    LEFT JOIN collateral_metadata cm
        ON cm.address = e.raw_asset_address
    -- Time-bounded price JOIN: partition pruning on prices.usd
    LEFT JOIN prices.usd p
        ON p.contract_address = e.raw_asset_address
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
