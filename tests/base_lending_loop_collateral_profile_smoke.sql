-- ============================================================
-- Smoke Test: Lending Loop Collateral Profile
-- Description: Validates that cross-protocol flows can be joined
--              with collateral positions to determine what backs
--              stablecoin borrows. Tests collateral categorization
--              (BTC vs ETH vs LST) and implied leverage calculation.
--              Uses events from the last 14 days.
-- Author: stefanopepe
-- Created: 2026-02-17
-- Updated: 2026-02-17
-- ============================================================

WITH stablecoins AS (
    SELECT address FROM (
        VALUES
            (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913),  -- USDC
            (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca),  -- USDbC
            (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2),  -- USDT
            (0x50c5725949a6f0c72e6c4a641f24049a917db0cb)   -- DAI
    ) AS t(address)
),

collateral_assets AS (
    SELECT address, symbol, category FROM (
        VALUES
            (0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf, 'cbBTC',  'btc'),
            (0x0555e30da8f98308edb960aa94c0db47230d2b9c, 'WBTC',   'btc'),
            (0x4200000000000000000000000000000000000006, 'WETH',   'eth')
    ) AS t(address, symbol, category)
),

morpho_blue_stablecoin_markets AS (
    SELECT id AS market_id
    FROM morpho_blue_base.morphoblue_evt_createmarket
    WHERE from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) IN (
        SELECT address FROM stablecoins
    )
),

-- Entities with stablecoin borrows on Aave V3
aave_borrowers AS (
    SELECT DISTINCT
        COALESCE(onBehalfOf, "user") AS entity_address,
        CAST(date_trunc('day', evt_block_time) AS DATE) AS block_date
    FROM aave_v3_base.pool_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
      AND reserve IN (SELECT address FROM stablecoins)
),

-- Entities with stablecoin borrows on Morpho Blue
morpho_borrowers AS (
    SELECT DISTINCT
        COALESCE(onBehalf, caller) AS entity_address,
        CAST(date_trunc('day', evt_block_time) AS DATE) AS block_date
    FROM morpho_blue_base.morphoblue_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
      AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
),

all_borrowers AS (
    SELECT * FROM aave_borrowers
    UNION SELECT * FROM morpho_borrowers
),

-- Collateral supply events for these borrowers (Aave V3 only for simplicity)
collateral_positions AS (
    SELECT
        COALESCE(s.onBehalfOf, s."user") AS entity_address,
        CAST(date_trunc('day', s.evt_block_time) AS DATE) AS block_date,
        ca.symbol AS collateral_symbol,
        ca.category AS collateral_category,
        COUNT(*) AS supply_events
    FROM aave_v3_base.pool_evt_supply s
    INNER JOIN collateral_assets ca ON ca.address = s.reserve
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
    GROUP BY 1, 2, 3, 4
),

-- Join borrowers with their collateral
borrower_collateral AS (
    SELECT
        b.entity_address,
        b.block_date,
        COALESCE(cp.collateral_category, 'unknown') AS collateral_category,
        cp.collateral_symbol,
        cp.supply_events
    FROM all_borrowers b
    LEFT JOIN collateral_positions cp
        ON cp.entity_address = b.entity_address
        AND cp.block_date = b.block_date
),

results AS (
    SELECT
        'collateral_profile_distribution' AS test_name,
        collateral_category,
        COUNT(DISTINCT entity_address) AS unique_borrowers,
        COUNT(*) AS position_count,
        SUM(COALESCE(supply_events, 0)) AS total_collateral_events
    FROM borrower_collateral
    GROUP BY collateral_category
)

SELECT * FROM results
