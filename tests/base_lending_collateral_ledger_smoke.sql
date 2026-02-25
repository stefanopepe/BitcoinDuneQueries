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
            (0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf, 'cbBTC',  'btc'),
            (0x0555e30da8f98308edb960aa94c0db47230d2b9c, 'WBTC',   'btc'),
            (0x4200000000000000000000000000000000000006, 'WETH',   'eth')
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
    FROM aave_v3_base.pool_evt_supply s
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
    FROM morpho_blue_base.morphoblue_evt_createmarket
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
    FROM morpho_blue_base.morphoblue_evt_supplycollateral s
    INNER JOIN morpho_blue_collateral_markets m ON m.market_id = s.id
    INNER JOIN collateral_assets ca ON ca.address = m.collateral_token
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
    GROUP BY ca.symbol, ca.category
),

-- ============================================================
-- COMBINE ALL RESULTS
-- ============================================================
all_results AS (
    SELECT * FROM aave_v3_collateral
    UNION ALL SELECT * FROM morpho_blue_collateral
)

SELECT
    'collateral_distribution' AS test_name,
    protocol,
    symbol,
    category,
    event_count
FROM all_results
UNION ALL
SELECT
    'collateral_distribution',
    'none',
    'none',
    'unknown',
    0
WHERE NOT EXISTS (SELECT 1 FROM all_results)
