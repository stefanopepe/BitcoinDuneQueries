-- ============================================================
-- Query: Lending Loop Collateral Profile (Nested Query)
-- Description: Joins cross-protocol flow data with collateral positions
--              to answer: "What backs the stablecoin borrows in lending loops?"
--              Produces daily aggregates of loop volume segmented by
--              collateral type (BTC, ETH, ETH LST).
-- Author: stefanopepe
-- Created: 2026-02-12
-- Updated: 2026-02-12
-- Architecture: V2 Nested Query (joins flow_stitching + collateral_ledger)
-- Dependencies: lending_flow_stitching (via query ID), lending_collateral_ledger (via query ID)
-- ============================================================
-- Output Columns:
--   block_date               - Date of flow activity
--   entity_address           - Entity executing the loop
--   source_protocol          - Protocol where stablecoin was borrowed
--   dest_protocol            - Protocol where stablecoin was supplied
--   stablecoin_symbol        - Borrowed/supplied stablecoin (USDC, USDT, etc.)
--   flow_amount_usd          - USD value of the stablecoin flow
--   collateral_category      - btc / eth / eth_lst / mixed / unknown
--   collateral_symbol        - Primary collateral asset symbol
--   collateral_amount_usd    - Total collateral value for this entity on this day
--   implied_leverage         - flow_amount_usd / collateral_amount_usd
--   is_btc_backed            - TRUE if any collateral is WBTC
-- ============================================================

WITH
-- Reference the flow stitching query (column-pruned)
flows AS (
    SELECT
        block_date,
        entity_address,
        source_protocol,
        dest_protocol,
        asset_symbol,
        amount_usd,
        is_same_tx
    FROM query_6690272
),

-- Reference the collateral ledger (column-pruned)
collateral AS (
    SELECT
        block_date,
        entity_address,
        protocol,
        action_type,
        collateral_address,
        collateral_symbol,
        collateral_category,
        amount,
        amount_usd
    FROM query_<COLLATERAL_LEDGER_QUERY_ID>
),

-- ============================================================
-- COMPUTE DAILY COLLATERAL POSITIONS PER ENTITY
-- Running cumulative collateral by entity + asset
-- ============================================================

daily_collateral_changes AS (
    SELECT
        block_date,
        entity_address,
        collateral_address,
        collateral_symbol,
        collateral_category,
        SUM(CASE
            WHEN action_type = 'supply_collateral' THEN COALESCE(amount_usd, 0)
            WHEN action_type = 'withdraw_collateral' THEN -COALESCE(amount_usd, 0)
            ELSE 0
        END) AS daily_collateral_change_usd,
        SUM(CASE
            WHEN action_type = 'supply_collateral' THEN COALESCE(amount, 0)
            WHEN action_type = 'withdraw_collateral' THEN -COALESCE(amount, 0)
            ELSE 0
        END) AS daily_collateral_change_native
    FROM collateral
    GROUP BY
        block_date,
        entity_address,
        collateral_address,
        collateral_symbol,
        collateral_category
),

-- Cumulative collateral per entity + asset over time
running_collateral AS (
    SELECT
        block_date,
        entity_address,
        collateral_address,
        collateral_symbol,
        collateral_category,
        SUM(daily_collateral_change_usd) OVER (
            PARTITION BY entity_address, collateral_address
            ORDER BY block_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_collateral_usd,
        SUM(daily_collateral_change_native) OVER (
            PARTITION BY entity_address, collateral_address
            ORDER BY block_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_collateral_native
    FROM daily_collateral_changes
),

-- Latest collateral snapshot per entity + asset (carry forward to flow dates)
-- For each entity and each date, get their collateral position
entity_collateral_snapshot AS (
    SELECT
        entity_address,
        block_date,
        collateral_symbol,
        collateral_category,
        cumulative_collateral_usd,
        cumulative_collateral_native
    FROM running_collateral
    WHERE cumulative_collateral_usd > 0  -- Only active positions
),

-- Aggregate across assets per entity per day
entity_daily_collateral AS (
    SELECT
        entity_address,
        block_date,
        SUM(cumulative_collateral_usd) AS total_collateral_usd,
        -- Primary collateral = highest USD value
        MAX_BY(collateral_symbol, cumulative_collateral_usd) AS primary_collateral_symbol,
        MAX_BY(collateral_category, cumulative_collateral_usd) AS primary_collateral_category,
        -- BTC flag
        MAX(CASE WHEN collateral_category = 'btc' THEN TRUE ELSE FALSE END) AS has_btc_collateral,
        -- Category breakdown
        SUM(CASE WHEN collateral_category = 'btc' THEN cumulative_collateral_usd ELSE 0 END) AS btc_collateral_usd,
        SUM(CASE WHEN collateral_category = 'eth' THEN cumulative_collateral_usd ELSE 0 END) AS eth_collateral_usd,
        SUM(CASE WHEN collateral_category = 'eth_lst' THEN cumulative_collateral_usd ELSE 0 END) AS eth_lst_collateral_usd,
        -- Mixed category flag
        COUNT(DISTINCT collateral_category) AS distinct_categories
    FROM entity_collateral_snapshot
    GROUP BY entity_address, block_date
),

-- ============================================================
-- JOIN FLOWS WITH COLLATERAL POSITIONS
-- For each cross-protocol flow, find the entity's collateral
-- on that date (or most recent prior date).
-- ============================================================

flows_with_collateral AS (
    SELECT
        f.block_date,
        f.entity_address,
        f.source_protocol,
        f.dest_protocol,
        f.asset_symbol AS stablecoin_symbol,
        f.amount_usd AS flow_amount_usd,
        f.is_same_tx,
        -- Collateral info (may be NULL if entity has no tracked collateral)
        ec.total_collateral_usd AS collateral_amount_usd,
        ec.primary_collateral_symbol AS collateral_symbol,
        CASE
            WHEN ec.entity_address IS NULL THEN 'unknown'
            WHEN ec.distinct_categories > 1 THEN 'mixed'
            ELSE ec.primary_collateral_category
        END AS collateral_category,
        ec.has_btc_collateral AS is_btc_backed,
        ec.btc_collateral_usd,
        ec.eth_collateral_usd,
        ec.eth_lst_collateral_usd,
        -- Implied leverage: flow volume / collateral
        CASE
            WHEN ec.total_collateral_usd > 0 THEN
                ROUND(f.amount_usd / ec.total_collateral_usd, 4)
            ELSE NULL
        END AS implied_leverage
    FROM flows f
    LEFT JOIN entity_daily_collateral ec
        ON ec.entity_address = f.entity_address
        AND ec.block_date = f.block_date
)

-- ============================================================
-- FINAL OUTPUT: Per-flow collateral attribution
-- ============================================================

SELECT
    block_date,
    entity_address,
    source_protocol,
    dest_protocol,
    stablecoin_symbol,
    flow_amount_usd,
    is_same_tx,
    collateral_category,
    collateral_symbol,
    COALESCE(collateral_amount_usd, 0) AS collateral_amount_usd,
    implied_leverage,
    COALESCE(is_btc_backed, FALSE) AS is_btc_backed,
    COALESCE(btc_collateral_usd, 0) AS btc_collateral_usd,
    COALESCE(eth_collateral_usd, 0) AS eth_collateral_usd,
    COALESCE(eth_lst_collateral_usd, 0) AS eth_lst_collateral_usd
FROM flows_with_collateral
ORDER BY block_date DESC, flow_amount_usd DESC
