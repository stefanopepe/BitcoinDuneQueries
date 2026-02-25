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
            (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),  -- USDC
            (0xdac17f958d2ee523a2206206994597c13d831ec7),  -- USDT
            (0x6b175474e89094c44da98b954eedeac495271d0f),  -- DAI
            (0x853d955acef822db058eb8505911ed77f175b99e)   -- FRAX
    ) AS t(address)
),

-- Resolve Morpho Blue stablecoin market IDs
morpho_blue_stablecoin_markets AS (
    SELECT id AS market_id
    FROM morpho_blue_ethereum.morphoblue_evt_createmarket
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
        FROM aave_v3_ethereum.pool_evt_borrow
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND reserve IN (SELECT address FROM stablecoins)
        UNION ALL
        SELECT COALESCE(onBehalfOf, "user"), 'aave_v3'
        FROM aave_v3_ethereum.pool_evt_supply
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND reserve IN (SELECT address FROM stablecoins)
        -- Morpho Blue
        UNION ALL
        SELECT COALESCE(onBehalf, caller), 'morpho_blue'
        FROM morpho_blue_ethereum.morphoblue_evt_borrow
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
        UNION ALL
        SELECT COALESCE(onBehalf, caller), 'morpho_blue'
        FROM morpho_blue_ethereum.morphoblue_evt_supply
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
        -- Compound V3
        UNION ALL
        SELECT src, 'compound_v3'
        FROM compound_v3_ethereum.comet_evt_withdraw
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND contract_address = 0xc3d688b66703497daa19211eedff47f25384cdc3
        UNION ALL
        SELECT dst, 'compound_v3'
        FROM compound_v3_ethereum.comet_evt_supply
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND contract_address = 0xc3d688b66703497daa19211eedff47f25384cdc3
        -- Compound V2
        UNION ALL
        SELECT borrower, 'compound_v2'
        FROM compound_ethereum.cerc20delegator_evt_borrow
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND contract_address IN (
              0x39aa39c021dfbae8fac545936693ac917d5e7563,
              0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
              0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
          )
        UNION ALL
        SELECT minter, 'compound_v2'
        FROM compound_ethereum.cerc20delegator_evt_mint
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND contract_address IN (
              0x39aa39c021dfbae8fac545936693ac917d5e7563,
              0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
              0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
          )
    )
    GROUP BY entity_address
    HAVING COUNT(DISTINCT protocol) >= 2
)

-- Results: entities active on 2+ protocols (potential loopers)
SELECT
    'multi_protocol_entity_count' AS test_name,
    protocol_count,
    COUNT(*) AS entity_count
FROM multi_protocol_entities
GROUP BY protocol_count
ORDER BY protocol_count
