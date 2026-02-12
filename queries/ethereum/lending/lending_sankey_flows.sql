-- ============================================================
-- Query: Lending Sankey Flows (Visualization Query)
-- Description: Produces an edge list dataset for Sankey diagram visualization
--              of cross-protocol lending flows. Each row represents a flow
--              from one protocol-action pair to another.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-05
-- Architecture: V2 Nested Query (aggregates flow_stitching)
-- Dependencies: lending_flow_stitching
-- ============================================================
-- Output Columns (Sankey Edge List Format):
--   day                  - Flow date (for filtering)
--   source               - Source node: {protocol}:{action}
--   target               - Target node: {protocol}:{action}
--   value                - Flow volume in USD
--   entity_count         - Number of unique entities
--   flow_count           - Number of individual flows
--   avg_time_delta       - Average seconds between borrow and supply
-- ============================================================
-- Sankey Node Format:
--   {protocol}:borrow:{asset_symbol}  -> Source of flow
--   {protocol}:supply:{asset_symbol}  -> Destination of flow
-- Example:
--   aave_v3:borrow:USDC -> morpho_blue:supply:USDC
-- ============================================================

WITH
-- Reference the flow stitching query (column-pruned)
flows AS (
    SELECT
        block_date,
        source_protocol,
        dest_protocol,
        asset_symbol,
        entity_address,
        amount_usd,
        time_delta_seconds,
        is_same_tx
    FROM query_6690272
),

-- ============================================================
-- AGGREGATE BY DAY + PROTOCOL PAIR + ASSET
-- ============================================================

daily_edges AS (
    SELECT
        block_date AS day,
        -- Source node: where borrow happened
        CONCAT(
            source_protocol, ':borrow:',
            COALESCE(asset_symbol, 'UNKNOWN')
        ) AS source,
        -- Target node: where supply happened
        CONCAT(
            dest_protocol, ':supply:',
            COALESCE(asset_symbol, 'UNKNOWN')
        ) AS target,
        -- Metrics
        SUM(COALESCE(amount_usd, 0)) AS value,
        COUNT(DISTINCT entity_address) AS entity_count,
        COUNT(*) AS flow_count,
        AVG(time_delta_seconds) AS avg_time_delta_seconds,
        -- Flow type breakdown
        COUNT(*) FILTER (WHERE is_same_tx) AS atomic_flows,
        COUNT(*) FILTER (WHERE NOT is_same_tx) AS cross_tx_flows
    FROM flows
    GROUP BY
        block_date,
        source_protocol,
        dest_protocol,
        asset_symbol
)

-- ============================================================
-- FINAL OUTPUT: Daily asset-level edges
-- ============================================================

SELECT
    day,
    source,
    target,
    value,
    entity_count,
    flow_count,
    ROUND(avg_time_delta_seconds, 1) AS avg_time_delta_seconds,
    atomic_flows,
    cross_tx_flows,
    'daily_asset_level' AS aggregation_level
FROM daily_edges
WHERE value > 0  -- Only include edges with actual volume
ORDER BY day DESC, value DESC
