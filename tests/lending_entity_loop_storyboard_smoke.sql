-- ============================================================
-- Smoke Test: Lending Entity Loop Storyboard
-- Description: Validates that per-entity time-ordered action traces
--              can be computed with running collateral/debt totals.
--              Focuses on multi-protocol entities using Aave V3
--              and Morpho Blue stablecoin events from the last 14 days.
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

morpho_blue_stablecoin_markets AS (
    SELECT id AS market_id
    FROM morpho_blue_ethereum.morphoblue_evt_createmarket
    WHERE from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) IN (
        SELECT address FROM stablecoins
    )
),

-- Collect actions from multiple protocols
actions AS (
    -- Aave V3 supply
    SELECT
        evt_block_time AS block_time,
        CAST(date_trunc('day', evt_block_time) AS DATE) AS block_date,
        evt_tx_hash AS tx_hash,
        evt_index,
        'aave_v3' AS protocol,
        'supply' AS action_type,
        COALESCE(onBehalfOf, "user") AS entity_address,
        CAST(amount AS DOUBLE) / 1e6 AS amount_usd  -- simplified
    FROM aave_v3_ethereum.pool_evt_supply
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    -- Aave V3 borrow
    SELECT
        evt_block_time, CAST(date_trunc('day', evt_block_time) AS DATE),
        evt_tx_hash, evt_index, 'aave_v3', 'borrow',
        COALESCE(onBehalfOf, "user"),
        CAST(amount AS DOUBLE) / 1e6
    FROM aave_v3_ethereum.pool_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    -- Morpho Blue supply
    SELECT
        evt_block_time, CAST(date_trunc('day', evt_block_time) AS DATE),
        evt_tx_hash, evt_index, 'morpho_blue', 'supply',
        COALESCE(onBehalf, caller),
        CAST(assets AS DOUBLE) / 1e6
    FROM morpho_blue_ethereum.morphoblue_evt_supply
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
      AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
    UNION ALL
    -- Morpho Blue borrow
    SELECT
        evt_block_time, CAST(date_trunc('day', evt_block_time) AS DATE),
        evt_tx_hash, evt_index, 'morpho_blue', 'borrow',
        COALESCE(onBehalf, caller),
        CAST(assets AS DOUBLE) / 1e6
    FROM morpho_blue_ethereum.morphoblue_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
      AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
),

-- Filter to multi-protocol entities
multi_protocol AS (
    SELECT entity_address
    FROM actions
    GROUP BY entity_address
    HAVING COUNT(DISTINCT protocol) >= 2
),

-- Build storyboard with running totals for multi-protocol entities
entity_actions AS (
    SELECT
        a.entity_address,
        a.block_time,
        a.protocol,
        a.action_type,
        a.amount_usd,
        ROW_NUMBER() OVER (
            PARTITION BY a.entity_address ORDER BY a.block_time, a.evt_index
        ) AS event_sequence,
        SUM(CASE WHEN a.action_type = 'supply' THEN a.amount_usd ELSE -a.amount_usd END) OVER (
            PARTITION BY a.entity_address ORDER BY a.block_time, a.evt_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_net_usd
    FROM actions a
    INNER JOIN multi_protocol m ON m.entity_address = a.entity_address
)

-- Validate: multi-protocol entities with storyboard traces
SELECT
    'storyboard_validation' AS test_name,
    COUNT(DISTINCT entity_address) AS multi_protocol_entities,
    COUNT(*) AS total_events,
    MAX(event_sequence) AS max_event_depth,
    AVG(event_sequence) AS avg_events_per_entity
FROM entity_actions
