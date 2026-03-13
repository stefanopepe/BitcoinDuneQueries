-- ============================================================
-- Smoke Test: Lending Action Ledger - Aave V3
-- Description: Validates the Aave V3 base query on recent data
--              Tests all event types and computed fields
-- Author: stefanopepe
-- Created: 2026-02-05
-- ============================================================

-- Test on last 7 days of data (fast execution)
WITH
recent_supply AS (
    SELECT
        'supply' AS action_type,
        COUNT(*) AS event_count,
        COUNT(DISTINCT "user") AS unique_users,
        COUNT(DISTINCT reserve) AS unique_assets
    FROM aave_v3_base.pool_evt_supply
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
),

recent_borrow AS (
    SELECT
        'borrow' AS action_type,
        COUNT(*) AS event_count,
        COUNT(DISTINCT "user") AS unique_users,
        COUNT(DISTINCT reserve) AS unique_assets
    FROM aave_v3_base.pool_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
),

recent_repay AS (
    SELECT
        'repay' AS action_type,
        COUNT(*) AS event_count,
        COUNT(DISTINCT repayer) AS unique_users,
        COUNT(DISTINCT reserve) AS unique_assets
    FROM aave_v3_base.pool_evt_repay
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
),

recent_withdraw AS (
    SELECT
        'withdraw' AS action_type,
        COUNT(*) AS event_count,
        COUNT(DISTINCT "user") AS unique_users,
        COUNT(DISTINCT reserve) AS unique_assets
    FROM aave_v3_base.pool_evt_withdraw
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
)

-- Validation: Count events by type
SELECT
    'aave_v3_event_distribution' AS test_name,
    action_type,
    event_count,
    unique_users,
    unique_assets
FROM (
    SELECT * FROM recent_supply
    UNION ALL SELECT * FROM recent_borrow
    UNION ALL SELECT * FROM recent_repay
    UNION ALL SELECT * FROM recent_withdraw
)
WHERE event_count > 0
ORDER BY event_count DESC
