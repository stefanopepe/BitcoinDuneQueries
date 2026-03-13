-- ============================================================
-- Smoke Test: Lending Loop Metrics Daily
-- Description: Validates daily loop aggregation by checking that
--              multi-protocol entities produce countable loop
--              activity with reasonable metric ranges.
--              Uses stablecoin events from the last 30 days.
-- Author: stefanopepe
-- Created: 2026-02-17
-- Updated: 2026-02-17
-- ============================================================

WITH stablecoins AS (
    SELECT address FROM (
        VALUES
            (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913),  -- USDC
            (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2),  -- USDT
            (0x50c5725949a6f0c72e6c4a641f24049a917db0cb),  -- DAI
            (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca)   -- USDbC
    ) AS t(address)
),

morpho_blue_stablecoin_markets AS (
    SELECT id AS market_id
    FROM morpho_blue_base.morphoblue_evt_createmarket
    WHERE from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) IN (
        SELECT address FROM stablecoins
    )
),

-- Collect borrow events across protocols
borrows AS (
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE) AS day,
        evt_tx_hash AS tx_hash,
        COALESCE(onBehalfOf, "user") AS entity_address,
        'aave_v3' AS protocol
    FROM aave_v3_base.pool_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE),
        evt_tx_hash,
        COALESCE(onBehalf, caller),
        'morpho_blue'
    FROM morpho_blue_base.morphoblue_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
    -- Compound protocols intentionally excluded for Base until validated
),

-- Collect supply events across protocols
supplies AS (
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE) AS day,
        evt_tx_hash AS tx_hash,
        COALESCE(onBehalfOf, "user") AS entity_address,
        'aave_v3' AS protocol
    FROM aave_v3_base.pool_evt_supply
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE),
        evt_tx_hash,
        COALESCE(onBehalf, caller),
        'morpho_blue'
    FROM morpho_blue_base.morphoblue_evt_supply
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
    -- Compound protocols intentionally excluded for Base until validated
),

-- Same-tx cross-protocol flows (proxy for loops)
same_tx_flows AS (
    SELECT
        b.day,
        b.entity_address,
        b.protocol AS source_protocol,
        s.protocol AS dest_protocol
    FROM borrows b
    INNER JOIN supplies s
        ON s.tx_hash = b.tx_hash
        AND s.entity_address = b.entity_address
        AND s.protocol != b.protocol
),

results AS (
    SELECT
        'daily_loop_metrics' AS test_name,
        day,
        COUNT(*) AS flow_count,
        COUNT(DISTINCT entity_address) AS unique_entities,
        COUNT(DISTINCT source_protocol || '->' || dest_protocol) AS distinct_pairs
    FROM same_tx_flows
    GROUP BY day
    ORDER BY day DESC
    LIMIT 10
)

SELECT * FROM results
