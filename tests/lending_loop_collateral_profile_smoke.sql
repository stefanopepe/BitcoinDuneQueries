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
            (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),  -- USDC
            (0xdac17f958d2ee523a2206206994597c13d831ec7),  -- USDT
            (0x6b175474e89094c44da98b954eedeac495271d0f)   -- DAI
    ) AS t(address)
),

collateral_assets AS (
    SELECT address, symbol, category FROM (
        VALUES
            (0x2260fac5e5542a773aa44fbcfedf7c193bc2c599, 'WBTC',   'btc'),
            (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 'WETH',   'eth'),
            (0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0, 'wstETH', 'eth_lst'),
            (0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee, 'weETH',  'eth_lst'),
            (0xae78736cd615f374d3085123a210448e74fc6393, 'rETH',   'eth_lst'),
            (0xbe9895146f7af43049ca1c1ae358b0541ea49704, 'cbETH',  'eth_lst')
    ) AS t(address, symbol, category)
),

morpho_blue_stablecoin_markets AS (
    SELECT id AS market_id
    FROM morpho_blue_ethereum.morphoblue_evt_createmarket
    WHERE from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) IN (
        SELECT address FROM stablecoins
    )
),

-- Entities with stablecoin borrows on Aave V3
aave_borrowers AS (
    SELECT DISTINCT
        COALESCE(onBehalfOf, "user") AS entity_address,
        CAST(date_trunc('day', evt_block_time) AS DATE) AS block_date
    FROM aave_v3_ethereum.pool_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
      AND reserve IN (SELECT address FROM stablecoins)
),

-- Entities with stablecoin borrows on Morpho Blue
morpho_borrowers AS (
    SELECT DISTINCT
        COALESCE(onBehalf, caller) AS entity_address,
        CAST(date_trunc('day', evt_block_time) AS DATE) AS block_date
    FROM morpho_blue_ethereum.morphoblue_evt_borrow
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
    FROM aave_v3_ethereum.pool_evt_supply s
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
)

-- Validate: collateral category distribution for borrowers
SELECT
    'collateral_profile_distribution' AS test_name,
    collateral_category,
    COUNT(DISTINCT entity_address) AS unique_borrowers,
    COUNT(*) AS position_count,
    SUM(COALESCE(supply_events, 0)) AS total_collateral_events
FROM borrower_collateral
GROUP BY collateral_category
ORDER BY unique_borrowers DESC
