-- ============================================================
-- Query: Lending Entity Loop Storyboard (Visualization Query)
-- Description: Time-ordered trace of lending actions per entity,
--              showing how a loop unfolds step by step. Includes
--              running collateral/debt totals for visualization.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-11
-- Architecture: V2 Nested Query (joins unified base with balance sheet)
-- Dependencies: lending_action_ledger_unified
-- ============================================================
-- Output Columns:
--   entity_address       - Entity executing the loop
--   event_sequence       - Order of events (1, 2, 3...)
--   block_time           - Event timestamp
--   block_date           - Event date
--   tx_hash              - Transaction hash
--   protocol             - Protocol where action occurred
--   action_type          - supply/borrow/repay/withdraw
--   asset_symbol         - Asset involved
--   amount               - Action amount
--   amount_usd           - USD value
--   running_collateral_usd - Cumulative collateral across all protocols
--   running_debt_usd     - Cumulative debt across all protocols
--   net_equity_usd       - Collateral - Debt
--   leverage_ratio       - Collateral / Equity (if positive equity)
-- ============================================================

WITH
-- Reference the unified action ledger (column-pruned)
base_actions AS (
    SELECT
        block_time,
        block_date,
        tx_hash,
        evt_index,
        protocol,
        action_type,
        entity_address,
        asset_symbol,
        amount,
        amount_usd
    FROM query_6687961
),

-- ============================================================
-- FILTER TO ENTITIES WITH CROSS-PROTOCOL ACTIVITY
-- (Those who have used at least 2 different protocols)
-- ============================================================

multi_protocol_entities AS (
    SELECT entity_address
    FROM base_actions
    WHERE entity_address IS NOT NULL
    GROUP BY entity_address
    HAVING COUNT(DISTINCT protocol) >= 2
),

-- ============================================================
-- GET ALL ACTIONS FOR MULTI-PROTOCOL ENTITIES
-- ============================================================

entity_actions AS (
    SELECT
        a.entity_address,
        a.block_time,
        a.block_date,
        a.tx_hash,
        a.evt_index,
        a.protocol,
        a.action_type,
        a.asset_symbol,
        a.amount,
        a.amount_usd,
        -- Sequence number per entity
        ROW_NUMBER() OVER (
            PARTITION BY a.entity_address
            ORDER BY a.block_time, a.evt_index
        ) AS event_sequence
    FROM base_actions a
    INNER JOIN multi_protocol_entities m
        ON m.entity_address = a.entity_address
),

-- ============================================================
-- COMPUTE RUNNING POSITION TOTALS
-- ============================================================

with_running_totals AS (
    SELECT
        entity_address,
        event_sequence,
        block_time,
        block_date,
        tx_hash,
        protocol,
        action_type,
        asset_symbol,
        amount,
        amount_usd,
        -- Running collateral (supply - withdraw - liquidation)
        SUM(CASE
            WHEN action_type = 'supply' THEN COALESCE(amount_usd, 0)
            WHEN action_type = 'withdraw' THEN -COALESCE(amount_usd, 0)
            ELSE 0
        END) OVER (
            PARTITION BY entity_address
            ORDER BY block_time, evt_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_collateral_usd,
        -- Running debt (borrow - repay - liquidation)
        -- Liquidation reduces debt (debt is repaid by liquidator)
        SUM(CASE
            WHEN action_type = 'borrow' THEN COALESCE(amount_usd, 0)
            WHEN action_type IN ('repay', 'liquidation') THEN -COALESCE(amount_usd, 0)
            ELSE 0
        END) OVER (
            PARTITION BY entity_address
            ORDER BY block_time, evt_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_debt_usd
    FROM entity_actions
)

SELECT
    entity_address,
    event_sequence,
    block_time,
    block_date,
    tx_hash,
    protocol,
    action_type,
    asset_symbol,
    amount,
    amount_usd,
    running_collateral_usd,
    running_debt_usd,
    running_collateral_usd - running_debt_usd AS net_equity_usd,
    -- Leverage ratio: Collateral / Equity (only if positive equity)
    CASE
        WHEN running_collateral_usd - running_debt_usd > 0 THEN
            ROUND(
                running_collateral_usd / (running_collateral_usd - running_debt_usd),
                2
            )
        ELSE NULL
    END AS leverage_ratio,
    -- Position health indicator
    CASE
        WHEN running_collateral_usd - running_debt_usd < 0 THEN 'underwater'
        WHEN running_collateral_usd > 0 AND running_debt_usd = 0 THEN 'collateral_only'
        WHEN running_collateral_usd = 0 AND running_debt_usd > 0 THEN 'debt_only'
        WHEN running_collateral_usd / NULLIF(running_debt_usd, 0) > 2 THEN 'healthy'
        WHEN running_collateral_usd / NULLIF(running_debt_usd, 0) > 1.5 THEN 'moderate'
        ELSE 'risky'
    END AS position_health
FROM with_running_totals
ORDER BY entity_address, event_sequence
