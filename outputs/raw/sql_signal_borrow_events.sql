WITH
stable_tokens AS (
  SELECT blockchain, contract_address, lower(symbol) AS symbol, decimals
  FROM tokens.erc20
  WHERE blockchain IN ('ethereum', 'base')
    AND lower(symbol) IN ('usdc', 'usdt', 'dai')
),
btc_tokens AS (
  SELECT blockchain, contract_address, lower(symbol) AS symbol
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
morpho_borrows AS (
  SELECT
    'ethereum' AS chain,
    'morpho_blue' AS venue,
    b.evt_block_time AS borrow_time,
    b.evt_tx_hash AS borrow_tx_hash,
    COALESCE(b.onBehalf, b.caller) AS borrower,
    COALESCE(b.onBehalf, b.caller) AS receiver,
    m.loan_token AS stable_token,
    CAST(b.assets AS DOUBLE) AS amount_raw
  FROM morpho_blue_ethereum.morphoblue_evt_borrow b
  JOIN morpho_markets_eth m ON m.market_id = b.id
  JOIN stable_tokens st ON st.blockchain = 'ethereum' AND st.contract_address = m.loan_token
  WHERE b.evt_block_time >= TIMESTAMP '2026-01-27 11:57:30'

  UNION ALL

  SELECT
    'base' AS chain,
    'morpho_blue' AS venue,
    b.evt_block_time AS borrow_time,
    b.evt_tx_hash AS borrow_tx_hash,
    COALESCE(b.onBehalf, b.caller) AS borrower,
    COALESCE(b.onBehalf, b.caller) AS receiver,
    m.loan_token AS stable_token,
    CAST(b.assets AS DOUBLE) AS amount_raw
  FROM morpho_blue_base.morphoblue_evt_borrow b
  JOIN morpho_markets_base m ON m.market_id = b.id
  JOIN stable_tokens st ON st.blockchain = 'base' AND st.contract_address = m.loan_token
  WHERE b.evt_block_time >= TIMESTAMP '2026-01-27 11:57:30'
),
morpho_btc_supplies AS (
  SELECT 'ethereum' AS chain, COALESCE(s.onBehalf, s.caller) AS borrower, s.evt_block_time AS action_time
  FROM morpho_blue_ethereum.morphoblue_evt_supplycollateral s
  JOIN morpho_markets_eth m ON m.market_id = s.id
  JOIN btc_tokens bt ON bt.blockchain = 'ethereum' AND bt.contract_address = m.collateral_token
  WHERE s.evt_block_time >= TIMESTAMP '2026-01-27 11:57:30' - INTERVAL '7' DAY

  UNION ALL

  SELECT 'base' AS chain, COALESCE(s.onBehalf, s.caller) AS borrower, s.evt_block_time AS action_time
  FROM morpho_blue_base.morphoblue_evt_supplycollateral s
  JOIN morpho_markets_base m ON m.market_id = s.id
  JOIN btc_tokens bt ON bt.blockchain = 'base' AND bt.contract_address = m.collateral_token
  WHERE s.evt_block_time >= TIMESTAMP '2026-01-27 11:57:30' - INTERVAL '7' DAY
),
cohort_borrows AS (
  SELECT b.*
  FROM morpho_borrows b
  WHERE EXISTS (
    SELECT 1
    FROM morpho_btc_supplies s
    WHERE s.chain = b.chain
      AND s.borrower = b.borrower
      AND s.action_time <= b.borrow_time
  )
),
borrows_with_usd AS (
  SELECT
    b.chain,
    b.venue,
    b.borrow_time,
    b.borrow_tx_hash,
    lower(to_hex(b.borrower)) AS borrower,
    lower(to_hex(b.receiver)) AS receiver,
    lower(st.symbol) AS stable_symbol,
    b.amount_raw / POWER(10, st.decimals) AS borrow_amount,
    COALESCE((b.amount_raw / POWER(10, st.decimals)) * p.price, 0.0) AS borrow_amount_usd
  FROM cohort_borrows b
  JOIN stable_tokens st ON st.blockchain = b.chain AND st.contract_address = b.stable_token
  LEFT JOIN prices.usd p
    ON p.blockchain = b.chain
   AND p.contract_address = b.stable_token
   AND p.minute = date_trunc('minute', b.borrow_time)
),
cohort_receivers AS (
  SELECT DISTINCT chain, receiver
  FROM (
    SELECT chain, lower(to_hex(receiver)) AS receiver
    FROM cohort_borrows
  ) r
),
stable_to_btc_swaps AS (
  SELECT
    t.blockchain AS chain,
    t.block_time,
    lower(to_hex(t.taker)) AS trader
  FROM dex.trades t
  JOIN cohort_receivers cr
    ON cr.chain = t.blockchain
   AND cr.receiver = lower(to_hex(t.taker))
  JOIN stable_tokens st ON st.blockchain = t.blockchain AND st.contract_address = t.token_sold_address
  JOIN btc_tokens bt ON bt.blockchain = t.blockchain AND bt.contract_address = t.token_bought_address
  WHERE t.blockchain IN ('ethereum', 'base')
    AND t.block_time >= TIMESTAMP '2026-01-27 11:57:30'
),
borrow_features AS (
  SELECT
    b.*,
    EXISTS (
      SELECT 1 FROM stable_to_btc_swaps s
      WHERE s.chain = b.chain
        AND s.trader = b.receiver
        AND s.block_time >= b.borrow_time
        AND s.block_time <= b.borrow_time + INTERVAL '1' HOUR
    ) AND EXISTS (
      SELECT 1 FROM morpho_btc_supplies bs
      WHERE bs.chain = b.chain
        AND lower(to_hex(bs.borrower)) = b.borrower
        AND bs.action_time >= b.borrow_time
        AND bs.action_time <= b.borrow_time + INTERVAL '1' HOUR
    ) AS loop_1h,
    EXISTS (
      SELECT 1 FROM stable_to_btc_swaps s
      WHERE s.chain = b.chain
        AND s.trader = b.receiver
        AND s.block_time >= b.borrow_time
        AND s.block_time <= b.borrow_time + INTERVAL '24' HOUR
    ) AND EXISTS (
      SELECT 1 FROM morpho_btc_supplies bs
      WHERE bs.chain = b.chain
        AND lower(to_hex(bs.borrower)) = b.borrower
        AND bs.action_time >= b.borrow_time
        AND bs.action_time <= b.borrow_time + INTERVAL '24' HOUR
    ) AS loop_24h,
    EXISTS (
      SELECT 1 FROM stable_to_btc_swaps s
      WHERE s.chain = b.chain
        AND s.trader = b.receiver
        AND s.block_time >= b.borrow_time
        AND s.block_time <= b.borrow_time + INTERVAL '7' DAY
    ) AND EXISTS (
      SELECT 1 FROM morpho_btc_supplies bs
      WHERE bs.chain = b.chain
        AND lower(to_hex(bs.borrower)) = b.borrower
        AND bs.action_time >= b.borrow_time
        AND bs.action_time <= b.borrow_time + INTERVAL '7' DAY
    ) AS loop_7d,
    EXISTS (
      SELECT 1 FROM stable_to_btc_swaps s
      WHERE s.chain = b.chain
        AND s.trader = b.receiver
        AND s.block_time >= b.borrow_time
        AND s.block_time <= b.borrow_time + INTERVAL '7' DAY
    ) AS defi_7d,
    FALSE AS bridge_7d
  FROM borrows_with_usd b
)
SELECT *
FROM borrow_features
WHERE borrow_amount_usd > 0
