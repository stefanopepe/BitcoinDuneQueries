WITH
btc_tokens AS (
  SELECT blockchain, contract_address
  FROM tokens.erc20
  WHERE blockchain IN ('ethereum','base')
    AND lower(symbol) IN ('wbtc','cbbtc','tbtc')
),
stable_tokens AS (
  SELECT blockchain, contract_address, decimals
  FROM tokens.erc20
  WHERE blockchain IN ('ethereum','base')
    AND lower(symbol) IN ('usdc','usdt','dai')
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
    CAST(b.assets AS DOUBLE) AS borrow_raw,
    m.loan_token AS stable_token
  FROM morpho_blue_ethereum.morphoblue_evt_borrow b
  JOIN morpho_markets_eth m ON m.market_id = b.id
  JOIN stable_tokens st ON st.blockchain='ethereum' AND st.contract_address=m.loan_token
  WHERE b.evt_block_time >= TIMESTAMP '{start_ts}'

  UNION ALL

  SELECT
    'base' AS chain,
    b.evt_block_time AS borrow_time,
    COALESCE(b.onBehalf, b.caller) AS borrower,
    b.evt_tx_hash AS borrow_tx_hash,
    CAST(b.assets AS DOUBLE) AS borrow_raw,
    m.loan_token AS stable_token
  FROM morpho_blue_base.morphoblue_evt_borrow b
  JOIN morpho_markets_base m ON m.market_id = b.id
  JOIN stable_tokens st ON st.blockchain='base' AND st.contract_address=m.loan_token
  WHERE b.evt_block_time >= TIMESTAMP '{start_ts}'
),
btc_supplies AS (
  SELECT
    'ethereum' AS chain,
    COALESCE(s.onBehalf, s.caller) AS borrower,
    s.evt_block_time AS supply_time
  FROM morpho_blue_ethereum.morphoblue_evt_supplycollateral s
  JOIN morpho_markets_eth m ON m.market_id = s.id
  JOIN btc_tokens bt ON bt.blockchain='ethereum' AND bt.contract_address = m.collateral_token
  WHERE s.evt_block_time >= TIMESTAMP '{start_ts}' - INTERVAL '1' DAY

  UNION ALL

  SELECT
    'base' AS chain,
    COALESCE(s.onBehalf, s.caller) AS borrower,
    s.evt_block_time AS supply_time
  FROM morpho_blue_base.morphoblue_evt_supplycollateral s
  JOIN morpho_markets_base m ON m.market_id = s.id
  JOIN btc_tokens bt ON bt.blockchain='base' AND bt.contract_address = m.collateral_token
  WHERE s.evt_block_time >= TIMESTAMP '{start_ts}' - INTERVAL '1' DAY
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
borrows_enriched AS (
  SELECT
    b.chain,
    b.borrow_time,
    lower(to_hex(b.borrower)) AS borrower,
    lower(to_hex(b.borrow_tx_hash)) AS borrow_tx_hash,
    b.borrow_raw / POWER(10, st.decimals) AS borrow_amount,
    COALESCE((b.borrow_raw / POWER(10, st.decimals)) * p.price, 0.0) AS borrow_amount_usd,
    EXISTS (
      SELECT 1 FROM btc_supplies s
      WHERE s.chain = b.chain
        AND s.borrower = b.borrower
        AND s.supply_time >= b.borrow_time
        AND s.supply_time <= b.borrow_time + INTERVAL '24' HOUR
    ) AS loop24_proxy
  FROM cohort_borrows b
  JOIN stable_tokens st ON st.blockchain=b.chain AND st.contract_address=b.stable_token
  LEFT JOIN prices.usd p
    ON p.blockchain=b.chain
   AND p.contract_address=b.stable_token
   AND p.minute=date_trunc('minute', b.borrow_time)
)
SELECT
  chain,
  COUNT(*) AS n_borrows,
  COUNT_IF(loop24_proxy) AS n_loop24,
  SUM(borrow_amount_usd) AS borrow_usd,
  SUM(CASE WHEN loop24_proxy THEN borrow_amount_usd ELSE 0 END) AS loop24_usd
FROM borrows_enriched
WHERE borrow_amount_usd > 0
GROUP BY 1
ORDER BY 1
