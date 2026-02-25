-- ============================================================
-- Smoke Test: Lending Sankey Flows
-- Description: Validates that cross-protocol flow edges can be
--              aggregated into source->target pairs with volume.
--              Tests the Sankey node format: {protocol}:{action}:{asset}.
--              Uses stablecoin events from the last 14 days.
-- Author: stefanopepe
-- Created: 2026-02-17
-- Updated: 2026-02-17
-- ============================================================

WITH stablecoins AS (
    SELECT address, symbol FROM (
        VALUES
            (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 'USDC'),
            (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2, 'USDT'),
            (0x50c5725949a6f0c72e6c4a641f24049a917db0cb, 'DAI')
    ) AS t(address, symbol)
),

morpho_blue_stablecoin_markets AS (
    SELECT id AS market_id
    FROM morpho_blue_base.morphoblue_evt_createmarket
    WHERE from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) IN (
        SELECT address FROM stablecoins
    )
),

-- Aave V3 borrows with asset symbol
aave_borrows AS (
    SELECT
        CAST(date_trunc('day', b.evt_block_time) AS DATE) AS day,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'aave_v3' AS protocol,
        COALESCE(b.onBehalfOf, b."user") AS entity_address,
        sc.symbol AS asset_symbol,
        CAST(b.amount AS DOUBLE) / 1e6 AS amount_usd  -- simplified
    FROM aave_v3_base.pool_evt_borrow b
    JOIN stablecoins sc ON sc.address = b.reserve
    WHERE b.evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
),

-- Aave V3 supplies with asset symbol
aave_supplies AS (
    SELECT
        CAST(date_trunc('day', s.evt_block_time) AS DATE) AS day,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'aave_v3' AS protocol,
        COALESCE(s.onBehalfOf, s."user") AS entity_address,
        sc.symbol AS asset_symbol
    FROM aave_v3_base.pool_evt_supply s
    JOIN stablecoins sc ON sc.address = s.reserve
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
),

-- Morpho Blue borrows
morpho_borrows AS (
    SELECT
        CAST(date_trunc('day', b.evt_block_time) AS DATE) AS day,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'morpho_blue' AS protocol,
        COALESCE(b.onBehalf, b.caller) AS entity_address,
        'USDC' AS asset_symbol,  -- simplified
        CAST(b.assets AS DOUBLE) / 1e6 AS amount_usd
    FROM morpho_blue_base.morphoblue_evt_borrow b
    WHERE b.evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
      AND b.id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
),

-- Morpho Blue supplies
morpho_supplies AS (
    SELECT
        CAST(date_trunc('day', s.evt_block_time) AS DATE) AS day,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'morpho_blue' AS protocol,
        COALESCE(s.onBehalf, s.caller) AS entity_address,
        'USDC' AS asset_symbol
    FROM morpho_blue_base.morphoblue_evt_supply s
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
      AND s.id IN (SELECT market_id FROM morpho_blue_stablecoin_markets)
),

-- Same-tx cross-protocol flows with Sankey edge format
all_borrows AS (
    SELECT * FROM aave_borrows
    UNION ALL SELECT * FROM morpho_borrows
),
all_supplies AS (
    SELECT * FROM aave_supplies
    UNION ALL SELECT * FROM morpho_supplies
),

sankey_edges AS (
    SELECT
        b.day,
        CONCAT(b.protocol, ':borrow:', b.asset_symbol) AS source,
        CONCAT(s.protocol, ':supply:', s.asset_symbol) AS target,
        b.amount_usd AS value,
        b.entity_address
    FROM all_borrows b
    INNER JOIN all_supplies s
        ON s.tx_hash = b.tx_hash
        AND s.entity_address = b.entity_address
        AND s.evt_index > b.evt_index
        AND s.protocol != b.protocol
),

results AS (
    SELECT
        'sankey_edge_distribution' AS test_name,
        source,
        target,
        COUNT(*) AS flow_count,
        COUNT(DISTINCT entity_address) AS unique_entities,
        SUM(value) AS total_volume_usd
    FROM sankey_edges
    GROUP BY source, target
)

SELECT * FROM results
UNION ALL
SELECT
    'sankey_edge_distribution',
    'none',
    'none',
    0,
    0,
    0
WHERE NOT EXISTS (SELECT 1 FROM results)
