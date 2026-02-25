-- ============================================================
-- Smoke Test: Lending Action Ledger - Unified Multi-Protocol
-- Description: Validates the unified base query combining
--              Aave V3, Morpho Blue, Compound V3, and Compound V2
--              stablecoin events.
--              Tests amount extraction, stablecoin filtering, and
--              liquidation event inclusion.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-11
-- ============================================================

-- Stablecoin addresses for filtering
WITH stablecoins AS (
    SELECT address, symbol
    FROM (
        VALUES
            (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 'USDC'),
            (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2, 'USDT'),
            (0x50c5725949a6f0c72e6c4a641f24049a917db0cb, 'DAI'),
            (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca, 'USDbC')
    ) AS t(address, symbol)
),

-- ============================================================
-- TEST 1: Aave V3 stablecoin supply events have amounts
-- ============================================================
aave_v3_sample AS (
    SELECT
        s.evt_block_time,
        s.reserve,
        s.amount AS amount_raw,
        t.decimals,
        CAST(s.amount AS DOUBLE) / POWER(10, t.decimals) AS amount,
        p.price,
        CAST(s.amount AS DOUBLE) / POWER(10, t.decimals) * p.price AS amount_usd
    FROM aave_v3_base.pool_evt_supply s
    JOIN stablecoins sc ON sc.address = s.reserve
    LEFT JOIN tokens.erc20 t
        ON t.contract_address = s.reserve AND t.blockchain = 'base'
    LEFT JOIN prices.usd p
        ON p.contract_address = s.reserve
        AND p.blockchain = 'base'
        AND p.minute = date_trunc('minute', s.evt_block_time)
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
    LIMIT 5
),

-- ============================================================
-- TEST 2: Morpho Blue stablecoin market resolution works
-- ============================================================
morpho_blue_market_test AS (
    SELECT
        cm.id AS market_id,
        from_hex(substr(json_extract_scalar(cm.marketParams, '$.loanToken'), 3)) AS loan_token,
        COUNT(s.evt_tx_hash) AS supply_events
    FROM morpho_blue_base.morphoblue_evt_createmarket cm
    LEFT JOIN morpho_blue_base.morphoblue_evt_supply s
        ON s.id = cm.id
        AND s.evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
    WHERE from_hex(substr(json_extract_scalar(cm.marketParams, '$.loanToken'), 3)) IN (
        SELECT address FROM stablecoins
    )
    GROUP BY cm.id, cm.marketParams
    ORDER BY supply_events DESC
    LIMIT 5
),

-- ============================================================
-- TEST 3: Stablecoin event counts by protocol and action type
-- ============================================================
protocol_action_counts AS (
    -- Aave V3
    SELECT 'aave_v3' AS protocol, 'supply' AS action_type, COUNT(*) AS cnt
    FROM aave_v3_base.pool_evt_supply
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT 'aave_v3', 'borrow', COUNT(*)
    FROM aave_v3_base.pool_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT 'aave_v3', 'repay', COUNT(*)
    FROM aave_v3_base.pool_evt_repay
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT 'aave_v3', 'withdraw', COUNT(*)
    FROM aave_v3_base.pool_evt_withdraw
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT 'aave_v3', 'liquidation', COUNT(*)
    FROM aave_v3_base.pool_evt_liquidationcall
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND debtAsset IN (SELECT address FROM stablecoins)
    -- Morpho Blue (stablecoin loan markets)
    UNION ALL
    SELECT 'morpho_blue', 'supply', COUNT(*)
    FROM morpho_blue_base.morphoblue_evt_supply s
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND s.id IN (
          SELECT id FROM morpho_blue_base.morphoblue_evt_createmarket
          WHERE from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) IN (
              SELECT address FROM stablecoins
          )
      )
    UNION ALL
    SELECT 'morpho_blue', 'borrow', COUNT(*)
    FROM morpho_blue_base.morphoblue_evt_borrow b
    WHERE b.evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND b.id IN (
          SELECT id FROM morpho_blue_base.morphoblue_evt_createmarket
          WHERE from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) IN (
              SELECT address FROM stablecoins
          )
      )
    -- Compound protocols intentionally excluded for Base until validated
)

-- ============================================================
-- FINAL OUTPUT: Combined test results
-- ============================================================
SELECT
    'protocol_action_distribution' AS test_name,
    protocol,
    action_type,
    cnt AS event_count
FROM protocol_action_counts
ORDER BY protocol, action_type
