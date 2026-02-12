-- ============================================================
-- Query: Lending Entity Balance Sheet (Nested Query)
-- Description: Computes running collateral and debt balances per entity,
--              protocol, and asset. Uses window functions over the
--              unified action ledger to track position changes over time.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-11
-- Architecture: V2 Nested Query (1-level deep from unified base)
-- Dependencies: lending_action_ledger_unified
-- ============================================================
-- Output Columns:
--   block_date           - Balance snapshot date
--   entity_address       - Canonical entity address
--   protocol             - Protocol identifier
--   asset_address        - Asset contract
--   asset_symbol         - Token symbol
--   collateral_change    - Daily collateral position change
--   debt_change          - Daily debt position change
--   cumulative_collateral - Running collateral balance
--   cumulative_debt      - Running debt balance
--   net_position         - Collateral - Debt
-- ============================================================

WITH
-- Reference the unified action ledger base query (column-pruned)
base_actions AS (
    SELECT
        block_date,
        tx_hash,
        entity_address,
        protocol,
        asset_address,
        asset_symbol,
        action_type,
        amount
    FROM query_6687961
),

-- Compute daily position changes per entity, protocol, asset
daily_changes AS (
    SELECT
        block_date,
        entity_address,
        protocol,
        asset_address,
        asset_symbol,
        -- Collateral increases with supply, decreases with withdraw
        SUM(CASE
            WHEN action_type = 'supply' THEN COALESCE(amount, 0)
            WHEN action_type = 'withdraw' THEN -COALESCE(amount, 0)
            ELSE 0
        END) AS collateral_change,
        -- Debt increases with borrow, decreases with repay/liquidation
        SUM(CASE
            WHEN action_type = 'borrow' THEN COALESCE(amount, 0)
            WHEN action_type IN ('repay', 'liquidation') THEN -COALESCE(amount, 0)
            ELSE 0
        END) AS debt_change,
        -- Count actions for activity metrics
        COUNT(*) AS action_count,
        COUNT(DISTINCT tx_hash) AS tx_count
    FROM base_actions
    WHERE entity_address IS NOT NULL
    GROUP BY
        block_date,
        entity_address,
        protocol,
        asset_address,
        asset_symbol
),

-- Compute running balances using window functions
running_balances AS (
    SELECT
        block_date,
        entity_address,
        protocol,
        asset_address,
        asset_symbol,
        collateral_change,
        debt_change,
        action_count,
        tx_count,
        -- Cumulative collateral position
        SUM(collateral_change) OVER (
            PARTITION BY entity_address, protocol, asset_address
            ORDER BY block_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_collateral,
        -- Cumulative debt position
        SUM(debt_change) OVER (
            PARTITION BY entity_address, protocol, asset_address
            ORDER BY block_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_debt
    FROM daily_changes
)

SELECT
    block_date,
    entity_address,
    protocol,
    asset_address,
    asset_symbol,
    collateral_change,
    debt_change,
    cumulative_collateral,
    cumulative_debt,
    cumulative_collateral - cumulative_debt AS net_position,
    action_count,
    tx_count
FROM running_balances
ORDER BY block_date DESC, entity_address, protocol, asset_address
