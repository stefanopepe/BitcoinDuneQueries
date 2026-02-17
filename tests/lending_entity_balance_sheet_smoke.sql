-- ============================================================
-- Smoke Test: Lending Entity Balance Sheet
-- Description: Validates that running collateral and debt balances
--              can be computed per entity, protocol, and asset.
--              Uses Aave V3 supply/borrow/repay/withdraw events
--              for stablecoins over the last 14 days.
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

-- Collect all Aave V3 actions (simplified, stablecoins only)
aave_actions AS (
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE) AS block_date,
        COALESCE(onBehalfOf, "user") AS entity_address,
        'aave_v3' AS protocol,
        reserve AS asset_address,
        'supply' AS action_type,
        CAST(amount AS DOUBLE) / 1e6 AS amount  -- USDC/USDT = 6 decimals
    FROM aave_v3_ethereum.pool_evt_supply
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE),
        COALESCE(onBehalfOf, "user"),
        'aave_v3',
        reserve,
        'borrow',
        CAST(amount AS DOUBLE) / 1e6
    FROM aave_v3_ethereum.pool_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE),
        "user",
        'aave_v3',
        reserve,
        'repay',
        CAST(amount AS DOUBLE) / 1e6
    FROM aave_v3_ethereum.pool_evt_repay
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE),
        "user",
        'aave_v3',
        reserve,
        'withdraw',
        CAST(amount AS DOUBLE) / 1e6
    FROM aave_v3_ethereum.pool_evt_withdraw
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
      AND reserve IN (SELECT address FROM stablecoins)
),

-- Daily position changes
daily_changes AS (
    SELECT
        block_date,
        entity_address,
        protocol,
        asset_address,
        SUM(CASE
            WHEN action_type = 'supply' THEN amount
            WHEN action_type = 'withdraw' THEN -amount
            ELSE 0
        END) AS collateral_change,
        SUM(CASE
            WHEN action_type = 'borrow' THEN amount
            WHEN action_type = 'repay' THEN -amount
            ELSE 0
        END) AS debt_change
    FROM aave_actions
    GROUP BY block_date, entity_address, protocol, asset_address
),

-- Running balances
running AS (
    SELECT
        block_date,
        entity_address,
        SUM(collateral_change) OVER (
            PARTITION BY entity_address, asset_address
            ORDER BY block_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_collateral,
        SUM(debt_change) OVER (
            PARTITION BY entity_address, asset_address
            ORDER BY block_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_debt
    FROM daily_changes
)

-- Validate: entities with both collateral and debt positions
SELECT
    'balance_sheet_validation' AS test_name,
    COUNT(DISTINCT entity_address) AS entities_with_positions,
    COUNT(*) AS total_position_rows,
    SUM(CASE WHEN cumulative_collateral > 0 AND cumulative_debt > 0 THEN 1 ELSE 0 END) AS entities_with_both,
    SUM(CASE WHEN cumulative_collateral - cumulative_debt < 0 THEN 1 ELSE 0 END) AS underwater_snapshots
FROM running
