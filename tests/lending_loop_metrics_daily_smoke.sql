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
            (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),  -- USDC
            (0xdac17f958d2ee523a2206206994597c13d831ec7),  -- USDT
            (0x6b175474e89094c44da98b954eedeac495271d0f),  -- DAI
            (0x853d955acef822db058eb8505911ed77f175b99e)   -- FRAX
    ) AS t(address)
),

morpho_blue_stablecoin_markets AS (
    SELECT id AS market_id
    FROM morpho_blue_ethereum.morphoblue_evt_createmarket
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
    FROM aave_v3_ethereum.pool_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE),
        evt_tx_hash,
        COALESCE(onBehalf, caller),
        'morpho_blue'
    FROM morpho_blue_ethereum.morphoblue_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
    UNION ALL
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE),
        evt_tx_hash,
        src,
        'compound_v3'
    FROM compound_v3_ethereum.comet_evt_withdraw
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND contract_address = 0xc3d688b66703497daa19211eedff47f25384cdc3
),

-- Collect supply events across protocols
supplies AS (
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE) AS day,
        evt_tx_hash AS tx_hash,
        COALESCE(onBehalfOf, "user") AS entity_address,
        'aave_v3' AS protocol
    FROM aave_v3_ethereum.pool_evt_supply
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE),
        evt_tx_hash,
        COALESCE(onBehalf, caller),
        'morpho_blue'
    FROM morpho_blue_ethereum.morphoblue_evt_supply
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
    UNION ALL
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE),
        evt_tx_hash,
        dst,
        'compound_v3'
    FROM compound_v3_ethereum.comet_evt_supply
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
      AND contract_address = 0xc3d688b66703497daa19211eedff47f25384cdc3
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
)

-- Validate: daily loop-like activity metrics
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
