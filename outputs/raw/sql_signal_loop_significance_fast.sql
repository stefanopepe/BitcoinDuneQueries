WITH
stable_tokens AS (
  SELECT address
  FROM (
    VALUES
      (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48), -- eth USDC
      (0xdac17f958d2ee523a2206206994597c13d831ec7), -- eth USDT
      (0x6b175474e89094c44da98b954eedeac495271d0f), -- eth DAI
      (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913), -- base USDC
      (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2), -- base USDT
      (0x50c5725949a6f0c72e6c4a641f24049a917db0cb)  -- base DAI
  ) AS t(address)
),
btc_tokens AS (
  SELECT blockchain, contract_address
  FROM tokens.erc20
  WHERE blockchain IN ('ethereum', 'base')
    AND lower(symbol) IN ('wbtc', 'cbbtc', 'tbtc')
),
morpho_markets_eth AS (
  SELECT
    id AS market_id,
    from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) AS loan_token,
    from_hex(substr(json_extract_scalar(marketParams, '$.collateralToken'), 3)) AS collateral_token
  FROM morpho_blue_ethereum.morphoblue_evt_createmarket
),
morpho_markets_base AS (
  SELECT
    id AS market_id,
    from_hex(substr(json_extract_scalar(marketParams, '$.loanToken'), 3)) AS loan_token,
    from_hex(substr(json_extract_scalar(marketParams, '$.collateralToken'), 3)) AS collateral_token
  FROM morpho_blue_base.morphoblue_evt_createmarket
),
borrows AS (
  SELECT
    'ethereum' AS chain,
    b.evt_block_time AS borrow_time,
    COALESCE(b.onBehalf, b.caller) AS borrower,
    b.evt_tx_hash AS borrow_tx_hash,
    m.loan_token
  FROM morpho_blue_ethereum.morphoblue_evt_borrow b
  JOIN morpho_markets_eth m ON m.market_id = b.id
  WHERE b.evt_block_time >= TIMESTAMP '2026-01-27 11:49:27'
    AND m.loan_token IN (SELECT address FROM stable_tokens)

  UNION ALL

  SELECT
    'base' AS chain,
    b.evt_block_time AS borrow_time,
    COALESCE(b.onBehalf, b.caller) AS borrower,
    b.evt_tx_hash AS borrow_tx_hash,
    m.loan_token
  FROM morpho_blue_base.morphoblue_evt_borrow b
  JOIN morpho_markets_base m ON m.market_id = b.id
  WHERE b.evt_block_time >= TIMESTAMP '2026-01-27 11:49:27'
    AND m.loan_token IN (SELECT address FROM stable_tokens)
),
btc_supplies AS (
  SELECT
    'ethereum' AS chain,
    COALESCE(s.onBehalf, s.caller) AS borrower,
    s.evt_block_time AS supply_time
  FROM morpho_blue_ethereum.morphoblue_evt_supplycollateral s
  JOIN morpho_markets_eth m ON m.market_id = s.id
  JOIN btc_tokens bt ON bt.blockchain='ethereum' AND bt.contract_address = m.collateral_token
  WHERE s.evt_block_time >= TIMESTAMP '2026-01-27 11:49:27' - INTERVAL '1' DAY

  UNION ALL

  SELECT
    'base' AS chain,
    COALESCE(s.onBehalf, s.caller) AS borrower,
    s.evt_block_time AS supply_time
  FROM morpho_blue_base.morphoblue_evt_supplycollateral s
  JOIN morpho_markets_base m ON m.market_id = s.id
  JOIN btc_tokens bt ON bt.blockchain='base' AND bt.contract_address = m.collateral_token
  WHERE s.evt_block_time >= TIMESTAMP '2026-01-27 11:49:27' - INTERVAL '1' DAY
),
cohort_borrows AS (
  SELECT b.*
  FROM borrows b
  WHERE EXISTS (
    SELECT 1 FROM btc_supplies pre
    WHERE pre.chain=b.chain
      AND pre.borrower=b.borrower
      AND pre.supply_time <= b.borrow_time
  )
),
loop_flags AS (
  SELECT
    b.chain,
    b.borrow_tx_hash,
    EXISTS (
      SELECT 1 FROM btc_supplies s
      WHERE s.chain = b.chain
        AND s.borrower = b.borrower
        AND s.supply_time >= b.borrow_time
        AND s.supply_time <= b.borrow_time + INTERVAL '24' HOUR
    ) AS loop24_proxy
  FROM cohort_borrows b
)
SELECT
  chain,
  COUNT(*) AS n_borrows,
  COUNT_IF(loop24_proxy) AS n_loop24,
  CAST(COUNT_IF(loop24_proxy) AS DOUBLE) / NULLIF(COUNT(*), 0) AS ls24_event_rate
FROM loop_flags
GROUP BY 1
ORDER BY 1
