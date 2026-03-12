-- ============================================================
-- Smoke Test: Lending Action Ledger - Morpho Blue (Base)
-- Description: Validates Morpho Blue lending event extraction on Base
--              across supply/borrow/repay/withdraw/liquidation actions.
-- Author: stefanopepe
-- Created: 2026-02-18
-- ============================================================

WITH stablecoins AS (
    SELECT address
    FROM (
        VALUES
            (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913),
            (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca),
            (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2),
            (0x50c5725949a6f0c72e6c4a641f24049a917db0cb)
    ) AS t(address)
),

morpho_blue_stablecoin_markets AS (
    SELECT id AS market_id
    FROM morpho_blue_base.morphoblue_evt_createmarket
    WHERE from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) IN (
        SELECT address FROM stablecoins
    )
),

protocol_action_counts AS (
    SELECT 'morpho_blue' AS protocol, 'supply' AS action_type, COUNT(*) AS cnt
    FROM morpho_blue_base.morphoblue_evt_supply
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
    UNION ALL
    SELECT 'morpho_blue', 'borrow', COUNT(*)
    FROM morpho_blue_base.morphoblue_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
    UNION ALL
    SELECT 'morpho_blue', 'repay', COUNT(*)
    FROM morpho_blue_base.morphoblue_evt_repay
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
    UNION ALL
    SELECT 'morpho_blue', 'withdraw', COUNT(*)
    FROM morpho_blue_base.morphoblue_evt_withdraw
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
    UNION ALL
    SELECT 'morpho_blue', 'liquidation', COUNT(*)
    FROM morpho_blue_base.morphoblue_evt_liquidate
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
)

SELECT
    'morpho_blue_event_distribution' AS test_name,
    action_type,
    cnt AS event_count
FROM protocol_action_counts
WHERE cnt > 0
ORDER BY event_count DESC
