-- ============================================================
-- Smoke Test: Lending Collateral Ledger
-- Description: Validates collateral event extraction across
--              Aave V3, Morpho Blue, Compound V3, and Compound V2.
--              Tests that supply_collateral events are found for
--              WBTC, WETH, and ETH LSTs across all protocols.
-- Author: stefanopepe
-- Created: 2026-02-12
-- Updated: 2026-02-12
-- ============================================================

-- Tracked collateral assets
WITH collateral_assets AS (
    SELECT address, symbol, category
    FROM (
        VALUES
            (0x2260fac5e5542a773aa44fbcfedf7c193bc2c599, 'WBTC',   'btc'),
            (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 'WETH',   'eth'),
            (0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0, 'wstETH', 'eth_lst'),
            (0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee, 'weETH',  'eth_lst'),
            (0xae78736cd615f374d3085123a210448e74fc6393, 'rETH',   'eth_lst'),
            (0xbe9895146f7af43049ca1c1ae358b0541ea49704, 'cbETH',  'eth_lst')
    ) AS t(address, symbol, category)
),

-- ============================================================
-- TEST 1: Aave V3 collateral supply events (non-stablecoin reserves)
-- ============================================================
aave_v3_collateral AS (
    SELECT
        'aave_v3' AS protocol,
        ca.symbol,
        ca.category,
        COUNT(*) AS event_count
    FROM aave_v3_ethereum.pool_evt_supply s
    JOIN collateral_assets ca ON ca.address = s.reserve
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
    GROUP BY ca.symbol, ca.category
),

-- ============================================================
-- TEST 2: Morpho Blue collateral market resolution + events
-- ============================================================
morpho_blue_collateral_markets AS (
    SELECT
        id AS market_id,
        from_hex(substr(json_extract_scalar(marketParams, '$.collateralToken'), 3)) AS collateral_token
    FROM morpho_blue_ethereum.morphoblue_evt_createmarket
    WHERE from_hex(substr(json_extract_scalar(marketParams, '$.collateralToken'), 3)) IN (
        SELECT address FROM collateral_assets
    )
),

morpho_blue_collateral AS (
    SELECT
        'morpho_blue' AS protocol,
        ca.symbol,
        ca.category,
        COUNT(*) AS event_count
    FROM morpho_blue_ethereum.morphoblue_evt_supplycollateral s
    INNER JOIN morpho_blue_collateral_markets m ON m.market_id = s.id
    INNER JOIN collateral_assets ca ON ca.address = m.collateral_token
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
    GROUP BY ca.symbol, ca.category
),

-- ============================================================
-- TEST 3: Compound V3 collateral supply events
-- ============================================================
compound_v3_collateral AS (
    SELECT
        'compound_v3' AS protocol,
        ca.symbol,
        ca.category,
        COUNT(*) AS event_count
    FROM compound_v3_ethereum.comet_evt_supplycollateral s
    INNER JOIN collateral_assets ca ON ca.address = s.asset
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND s.contract_address = 0xc3d688b66703497daa19211eedff47f25384cdc3
    GROUP BY ca.symbol, ca.category
),

-- ============================================================
-- TEST 4: Compound V2 cWBTC supply events
-- ============================================================
compound_v2_collateral AS (
    SELECT
        'compound_v2' AS protocol,
        'WBTC' AS symbol,
        'btc' AS category,
        COUNT(*) AS event_count
    FROM compound_ethereum.cerc20delegator_evt_mint
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND contract_address = 0xccf4429db6322d5c611ee964527d42e5d685dd6a  -- cWBTC2
),

-- ============================================================
-- COMBINE ALL RESULTS
-- ============================================================
all_results AS (
    SELECT * FROM aave_v3_collateral
    UNION ALL SELECT * FROM morpho_blue_collateral
    UNION ALL SELECT * FROM compound_v3_collateral
    UNION ALL SELECT * FROM compound_v2_collateral
)

SELECT
    'collateral_distribution' AS test_name,
    protocol,
    symbol,
    category,
    event_count
FROM all_results
WHERE event_count > 0
ORDER BY protocol, category, symbol
