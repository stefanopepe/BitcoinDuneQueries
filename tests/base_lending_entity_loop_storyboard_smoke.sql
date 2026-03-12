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
    SELECT address, decimals FROM (
        VALUES
            (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 6),   -- USDC
            (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca, 6),   -- USDbC
            (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2, 6),   -- USDT
            (0x50c5725949a6f0c72e6c4a641f24049a917db0cb, 18)   -- DAI
    ) AS t(address, decimals)
),

morpho_blue_stablecoin_markets AS (
    SELECT
        id AS market_id,
        from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) AS loan_token
    FROM morpho_blue_base.morphoblue_evt_createmarket
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
        CAST(amount AS DOUBLE) / POWER(10, sc.decimals) AS amount_usd
    FROM aave_v3_base.pool_evt_supply
    JOIN stablecoins sc ON sc.address = reserve
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
    UNION ALL
    -- Aave V3 borrow
    SELECT
        evt_block_time, CAST(date_trunc('day', evt_block_time) AS DATE),
        evt_tx_hash, evt_index, 'aave_v3', 'borrow',
        COALESCE(onBehalfOf, "user"),
        CAST(amount AS DOUBLE) / POWER(10, sc.decimals)
    FROM aave_v3_base.pool_evt_borrow
    JOIN stablecoins sc ON sc.address = reserve
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
    UNION ALL
    -- Morpho Blue supply
    SELECT
        evt_block_time, CAST(date_trunc('day', evt_block_time) AS DATE),
        evt_tx_hash, evt_index, 'morpho_blue', 'supply',
        COALESCE(onBehalf, caller),
        CAST(assets AS DOUBLE) / POWER(10, sc.decimals)
    FROM morpho_blue_base.morphoblue_evt_supply
    JOIN morpho_blue_stablecoin_markets m ON m.market_id = id
    JOIN stablecoins sc ON sc.address = m.loan_token
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
    UNION ALL
    -- Morpho Blue borrow
    SELECT
        evt_block_time, CAST(date_trunc('day', evt_block_time) AS DATE),
        evt_tx_hash, evt_index, 'morpho_blue', 'borrow',
        COALESCE(onBehalf, caller),
        CAST(assets AS DOUBLE) / POWER(10, sc.decimals)
    FROM morpho_blue_base.morphoblue_evt_borrow
    JOIN morpho_blue_stablecoin_markets m ON m.market_id = id
    JOIN stablecoins sc ON sc.address = m.loan_token
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
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
HAVING COUNT(*) > 0
