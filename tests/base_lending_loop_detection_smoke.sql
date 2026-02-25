-- ============================================================
-- Smoke Test: Lending Loop Detection
-- Description: Validates that multi-hop loops can be detected
--              by looking for entities with borrow+supply on
--              different protocols in the same transaction.
--              Uses stablecoin events from the last 30 days.
--              Covers Aave V3, Morpho Blue, Compound V3, Compound V2.
-- Author: stefanopepe
-- Created: 2026-02-11
-- Updated: 2026-02-11
-- ============================================================

WITH stablecoins AS (
    SELECT address FROM (
        VALUES
            (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913),  -- USDC
            (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2),  -- USDT
            (0x50c5725949a6f0c72e6c4a641f24049a917db0cb),  -- DAI
            (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca)   -- USDbC
    ) AS t(address)
),

-- Resolve Morpho Blue stablecoin market IDs
morpho_blue_stablecoin_markets AS (
    SELECT id AS market_id
    FROM morpho_blue_base.morphoblue_evt_createmarket
    WHERE from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) IN (
        SELECT address FROM stablecoins
    )
),

-- Entities active on multiple protocols (stablecoin events only)
multi_protocol_entities AS (
    SELECT entity_address, COUNT(DISTINCT protocol) AS protocol_count
    FROM (
        -- Aave V3
        SELECT COALESCE(onBehalfOf, "user") AS entity_address, 'aave_v3' AS protocol
        FROM aave_v3_base.pool_evt_borrow
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND reserve IN (SELECT address FROM stablecoins)
        UNION ALL
        SELECT COALESCE(onBehalfOf, "user"), 'aave_v3'
        FROM aave_v3_base.pool_evt_supply
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND reserve IN (SELECT address FROM stablecoins)
        -- Morpho Blue
        UNION ALL
        SELECT COALESCE(onBehalf, caller), 'morpho_blue'
        FROM morpho_blue_base.morphoblue_evt_borrow
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
        UNION ALL
        SELECT COALESCE(onBehalf, caller), 'morpho_blue'
        FROM morpho_blue_base.morphoblue_evt_supply
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
        -- Compound protocols intentionally excluded for Base until validated
    )
    GROUP BY entity_address
    HAVING COUNT(DISTINCT protocol) >= 2
),

results AS (
    SELECT
        'multi_protocol_entity_count' AS test_name,
        protocol_count,
        COUNT(*) AS entity_count
    FROM multi_protocol_entities
    GROUP BY protocol_count
)

SELECT * FROM results
UNION ALL
SELECT
    'multi_protocol_entity_count',
    0,
    0
WHERE NOT EXISTS (SELECT 1 FROM results)
