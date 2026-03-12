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
    SELECT address, symbol, decimals FROM (
        VALUES
            (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 'USDC', 6),
            (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca, 'USDbC', 6),
            (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2, 'USDT', 6),
            (0x50c5725949a6f0c72e6c4a641f24049a917db0cb, 'DAI', 18)
    ) AS t(address, symbol, decimals)
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

-- Aave V3 borrows with asset symbol
aave_borrows AS (
    SELECT
        CAST(date_trunc('day', b.evt_block_time) AS DATE) AS day,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'aave_v3' AS protocol,
        COALESCE(b.onBehalfOf, b."user") AS entity_address,
        sc.symbol AS asset_symbol,
        CAST(b.amount AS DOUBLE) / POWER(10, sc.decimals) AS amount_usd
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
        sc.symbol AS asset_symbol,
        CAST(b.assets AS DOUBLE) / POWER(10, sc.decimals) AS amount_usd
    FROM morpho_blue_base.morphoblue_evt_borrow b
    JOIN morpho_blue_stablecoin_markets m ON m.market_id = b.id
    JOIN stablecoins sc ON sc.address = m.loan_token
    WHERE b.evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
),

-- Morpho Blue supplies
morpho_supplies AS (
    SELECT
        CAST(date_trunc('day', s.evt_block_time) AS DATE) AS day,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'morpho_blue' AS protocol,
        COALESCE(s.onBehalf, s.caller) AS entity_address,
        sc.symbol AS asset_symbol
    FROM morpho_blue_base.morphoblue_evt_supply s
    JOIN morpho_blue_stablecoin_markets m ON m.market_id = s.id
    JOIN stablecoins sc ON sc.address = m.loan_token
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '14' DAY
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
