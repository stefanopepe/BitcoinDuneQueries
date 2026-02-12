-- ============================================================
-- Query: Lending Action Ledger - Unified Multi-Protocol (Base Query)
-- Description: Unified action ledger combining Aave V3, Morpho Blue,
--              Compound V3, and Compound V2 lending events into a
--              single normalized schema.
--              Scoped to stablecoins (USDC, USDT, DAI, FRAX).
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
--   - compound_v3: Compound V3 Comet USDC (9k borrows/90d)
--   - compound_v2: Compound V2 legacy (low activity, kept for coverage)
-- ============================================================
-- Asset Scope: Stablecoins only
--   - USDC  0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
--   - USDT  0xdac17f958d2ee523a2206206994597c13d831ec7
--   - DAI   0x6b175474e89094c44da98b954eedeac495271d0f
--   - FRAX  0x853d955acef822db058eb8505911ed77f175b99e
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
            amount_raw UINT256,
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
            (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 'USDC',  6),
            (0xdac17f958d2ee523a2206206994597c13d831ec7, 'USDT',  6),
            (0x6b175474e89094c44da98b954eedeac495271d0f, 'DAI',  18),
            (0x853d955acef822db058eb8505911ed77f175b99e, 'FRAX',  18)
    ) AS t(address, symbol, decimals)
),

stablecoins AS (
    SELECT address FROM stablecoin_metadata
),

-- Map wrapper tokens to underlying (Compound V2 cTokens, Compound V3 Comet)
wrapper_to_underlying AS (
    SELECT wrapper, underlying
    FROM (
        VALUES
            -- Compound V2 cTokens
            (0x39aa39c021dfbae8fac545936693ac917d5e7563, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),  -- cUSDC  -> USDC
            (0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9, 0xdac17f958d2ee523a2206206994597c13d831ec7),  -- cUSDT  -> USDT
            (0x5d3a536e4d6dbd6114cc1ead35777bab948e3643, 0x6b175474e89094c44da98b954eedeac495271d0f),  -- cDAI   -> DAI
            -- Compound V3 Comet contracts
            (0xc3d688b66703497daa19211eedff47f25384cdc3, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48)   -- cUSDCv3 -> USDC
    ) AS t(wrapper, underlying)
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
        CAST(json_extract_scalar(marketParams, '$.loanToken') AS VARBINARY) AS loan_token
    FROM morpho_blue_ethereum.morphoblue_evt_createmarket
    WHERE CAST(json_extract_scalar(marketParams, '$.loanToken') AS VARBINARY) IN (
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
    FROM morpho_blue_ethereum.morphoblue_evt_supply s
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
    FROM morpho_blue_ethereum.morphoblue_evt_borrow b
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
    FROM morpho_blue_ethereum.morphoblue_evt_repay r
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
    FROM morpho_blue_ethereum.morphoblue_evt_withdraw w
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
    FROM morpho_blue_ethereum.morphoblue_evt_liquidate l
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
    FROM aave_v3_ethereum.pool_evt_supply s
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
    FROM aave_v3_ethereum.pool_evt_borrow b
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
    FROM aave_v3_ethereum.pool_evt_repay r
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
    FROM aave_v3_ethereum.pool_evt_withdraw w
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
    FROM aave_v3_ethereum.pool_evt_liquidationcall l
    CROSS JOIN checkpoint c
    WHERE l.evt_block_date >= c.cutoff_date
      AND l.evt_block_date < CURRENT_DATE
      AND l.debtAsset IN (SELECT address FROM stablecoins)
),

-- ============================================================
-- COMPOUND V3 (COMET) EVENTS
-- Compound V3 has one Comet contract per base asset.
-- USDC Comet: 0xc3d688b66703497daa19211eedff47f25384cdc3
-- Supply event = lending base asset; Withdraw event = borrowing base asset.
-- No separate borrow/repay events — they use supply/withdraw.
-- ============================================================

compound_v3_supply AS (
    SELECT
        s.evt_block_time AS block_time,
        s.evt_block_date AS block_date,
        s.evt_block_number AS block_number,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'compound_v3' AS protocol,
        'supply' AS action_type,
        s."from" AS user_address,
        s.dst AS on_behalf_of,
        s.contract_address AS raw_asset_address,
        s.amount AS amount_raw
    FROM compound_v3_ethereum.comet_evt_supply s
    CROSS JOIN checkpoint c
    WHERE s.evt_block_date >= c.cutoff_date
      AND s.evt_block_date < CURRENT_DATE
      -- USDC Comet only
      AND s.contract_address = 0xc3d688b66703497daa19211eedff47f25384cdc3
),

compound_v3_borrow AS (
    SELECT
        w.evt_block_time AS block_time,
        w.evt_block_date AS block_date,
        w.evt_block_number AS block_number,
        w.evt_tx_hash AS tx_hash,
        w.evt_index,
        'compound_v3' AS protocol,
        'borrow' AS action_type,
        w.src AS user_address,
        w.src AS on_behalf_of,
        w.contract_address AS raw_asset_address,
        w.amount AS amount_raw
    FROM compound_v3_ethereum.comet_evt_withdraw w
    CROSS JOIN checkpoint c
    WHERE w.evt_block_date >= c.cutoff_date
      AND w.evt_block_date < CURRENT_DATE
      AND w.contract_address = 0xc3d688b66703497daa19211eedff47f25384cdc3
),

compound_v3_liquidation AS (
    SELECT
        l.evt_block_time AS block_time,
        l.evt_block_date AS block_date,
        l.evt_block_number AS block_number,
        l.evt_tx_hash AS tx_hash,
        l.evt_index,
        'compound_v3' AS protocol,
        'liquidation' AS action_type,
        l.absorber AS user_address,
        l.borrower AS on_behalf_of,
        -- AbsorbDebt is denominated in base asset (USDC)
        0xc3d688b66703497daa19211eedff47f25384cdc3 AS raw_asset_address,
        CAST(l.basePaidOut AS UINT256) AS amount_raw
    FROM compound_v3_ethereum.comet_evt_absorbdebt l
    CROSS JOIN checkpoint c
    WHERE l.evt_block_date >= c.cutoff_date
      AND l.evt_block_date < CURRENT_DATE
      AND l.contract_address = 0xc3d688b66703497daa19211eedff47f25384cdc3
),

-- ============================================================
-- COMPOUND V2 EVENTS (legacy, low activity but kept for coverage)
-- Uses cTokens (wrapper addresses) and per-event amount columns
-- ============================================================

compound_v2_supply AS (
    SELECT
        m.evt_block_time AS block_time,
        m.evt_block_date AS block_date,
        m.evt_block_number AS block_number,
        m.evt_tx_hash AS tx_hash,
        m.evt_index,
        'compound_v2' AS protocol,
        'supply' AS action_type,
        m.minter AS user_address,
        m.minter AS on_behalf_of,
        m.contract_address AS raw_asset_address,
        m.mintAmount AS amount_raw
    FROM compound_ethereum.cerc20delegator_evt_mint m
    CROSS JOIN checkpoint c
    WHERE m.evt_block_date >= c.cutoff_date
      AND m.evt_block_date < CURRENT_DATE
      AND m.contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,  -- cUSDC
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,  -- cUSDT
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643   -- cDAI
      )
),

compound_v2_borrow AS (
    SELECT
        b.evt_block_time AS block_time,
        b.evt_block_date AS block_date,
        b.evt_block_number AS block_number,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'compound_v2' AS protocol,
        'borrow' AS action_type,
        b.borrower AS user_address,
        b.borrower AS on_behalf_of,
        b.contract_address AS raw_asset_address,
        b.borrowAmount AS amount_raw
    FROM compound_ethereum.cerc20delegator_evt_borrow b
    CROSS JOIN checkpoint c
    WHERE b.evt_block_date >= c.cutoff_date
      AND b.evt_block_date < CURRENT_DATE
      AND b.contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
      )
),

compound_v2_repay AS (
    SELECT
        r.evt_block_time AS block_time,
        r.evt_block_date AS block_date,
        r.evt_block_number AS block_number,
        r.evt_tx_hash AS tx_hash,
        r.evt_index,
        'compound_v2' AS protocol,
        'repay' AS action_type,
        r.payer AS user_address,
        r.borrower AS on_behalf_of,
        r.contract_address AS raw_asset_address,
        r.repayAmount AS amount_raw
    FROM compound_ethereum.cerc20delegator_evt_repayborrow r
    CROSS JOIN checkpoint c
    WHERE r.evt_block_date >= c.cutoff_date
      AND r.evt_block_date < CURRENT_DATE
      AND r.contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
      )
),

compound_v2_withdraw AS (
    SELECT
        r.evt_block_time AS block_time,
        r.evt_block_date AS block_date,
        r.evt_block_number AS block_number,
        r.evt_tx_hash AS tx_hash,
        r.evt_index,
        'compound_v2' AS protocol,
        'withdraw' AS action_type,
        r.redeemer AS user_address,
        r.redeemer AS on_behalf_of,
        r.contract_address AS raw_asset_address,
        r.redeemAmount AS amount_raw
    FROM compound_ethereum.cerc20delegator_evt_redeem r
    CROSS JOIN checkpoint c
    WHERE r.evt_block_date >= c.cutoff_date
      AND r.evt_block_date < CURRENT_DATE
      AND r.contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
      )
),

compound_v2_liquidation AS (
    SELECT
        l.evt_block_time AS block_time,
        l.evt_block_date AS block_date,
        l.evt_block_number AS block_number,
        l.evt_tx_hash AS tx_hash,
        l.evt_index,
        'compound_v2' AS protocol,
        'liquidation' AS action_type,
        l.liquidator AS user_address,
        l.borrower AS on_behalf_of,
        l.contract_address AS raw_asset_address,
        l.repayAmount AS amount_raw
    FROM compound_ethereum.cerc20delegator_evt_liquidateborrow l
    CROSS JOIN checkpoint c
    WHERE l.evt_block_date >= c.cutoff_date
      AND l.evt_block_date < CURRENT_DATE
      AND l.contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
      )
),

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
    -- Compound V3
    UNION ALL SELECT * FROM compound_v3_supply
    UNION ALL SELECT * FROM compound_v3_borrow
    UNION ALL SELECT * FROM compound_v3_liquidation
    -- Compound V2
    UNION ALL SELECT * FROM compound_v2_supply
    UNION ALL SELECT * FROM compound_v2_borrow
    UNION ALL SELECT * FROM compound_v2_repay
    UNION ALL SELECT * FROM compound_v2_withdraw
    UNION ALL SELECT * FROM compound_v2_liquidation
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
        e.amount_raw,
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
        AND p.blockchain = 'ethereum'
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
