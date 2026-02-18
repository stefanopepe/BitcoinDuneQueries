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
    SELECT address, decimals FROM (
        VALUES
            (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 6),   -- USDC
            (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca, 6),   -- USDbC
            (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2, 6),   -- USDT
            (0x50c5725949a6f0c72e6c4a641f24049a917db0cb, 18)   -- DAI
    ) AS t(address, decimals)
),

-- Collect all Aave V3 actions (simplified, stablecoins only)
aave_actions AS (
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE) AS block_date,
        COALESCE(onBehalfOf, "user") AS entity_address,
        'aave_v3' AS protocol,
        reserve AS asset_address,
        'supply' AS action_type,
        CAST(amount AS DOUBLE) / POWER(10, sc.decimals) AS amount
    FROM aave_v3_base.pool_evt_supply
    JOIN stablecoins sc ON sc.address = reserve
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
    UNION ALL
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE),
        COALESCE(onBehalfOf, "user"),
        'aave_v3',
        reserve,
        'borrow',
        CAST(amount AS DOUBLE) / POWER(10, sc.decimals)
    FROM aave_v3_base.pool_evt_borrow
    JOIN stablecoins sc ON sc.address = reserve
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
    UNION ALL
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE),
        "user",
        'aave_v3',
        reserve,
        'repay',
        CAST(amount AS DOUBLE) / POWER(10, sc.decimals)
    FROM aave_v3_base.pool_evt_repay
    JOIN stablecoins sc ON sc.address = reserve
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
    UNION ALL
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE),
        "user",
        'aave_v3',
        reserve,
        'withdraw',
        CAST(amount AS DOUBLE) / POWER(10, sc.decimals)
    FROM aave_v3_base.pool_evt_withdraw
    JOIN stablecoins sc ON sc.address = reserve
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
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
HAVING COUNT(*) > 0
