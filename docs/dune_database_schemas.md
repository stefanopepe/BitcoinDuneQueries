# Dune Analytics Database Schemas Reference

This document provides a quick reference for commonly used Dune Analytics table schemas. Use this as a reliable offline reference when building queries.

> **Last Updated:** 2026-01-30

---

## Table of Contents

- [Ethereum Raw Tables](#ethereum-raw-tables)
- [Bitcoin Raw Tables](#bitcoin-raw-tables)
- [Other EVM Chains](#other-evm-chains)
- [Token Metadata](#token-metadata)
- [Price Tables](#price-tables)
- [Spellbook Tables](#spellbook-tables)
- [Common Decoded Tables](#common-decoded-tables)
- [Useful Functions](#useful-functions)

---

## Ethereum Raw Tables

### ethereum.transactions

All transactions on Ethereum mainnet.

| Column | Type | Description |
|--------|------|-------------|
| `block_time` | TIMESTAMP | Block timestamp |
| `block_number` | BIGINT | Block number |
| `block_hash` | VARBINARY | Block hash |
| `hash` | VARBINARY | Transaction hash |
| `nonce` | BIGINT | Sender nonce |
| `index` | BIGINT | Transaction index in block |
| `from` | VARBINARY | Sender address (20 bytes) |
| `to` | VARBINARY | Recipient address (NULL for contract creation) |
| `value` | UINT256 | Value transferred in wei |
| `gas_limit` | BIGINT | Gas limit |
| `gas_price` | BIGINT | Gas price in wei |
| `gas_used` | BIGINT | Gas actually used |
| `data` | VARBINARY | Input data (calldata) |
| `max_fee_per_gas` | BIGINT | EIP-1559 max fee per gas |
| `max_priority_fee_per_gas` | BIGINT | EIP-1559 priority fee |
| `priority_fee_per_gas` | BIGINT | Actual priority fee paid |
| `type` | VARCHAR | Transaction type (legacy, eip1559, etc.) |
| `access_list` | ARRAY | EIP-2930 access list |
| `success` | BOOLEAN | Whether transaction succeeded |

**Common Filters:**
```sql
WHERE block_time >= DATE '2024-01-01'
  AND success = TRUE
  AND value > 0
```

### ethereum.traces

Internal transactions (calls between contracts).

| Column | Type | Description |
|--------|------|-------------|
| `block_time` | TIMESTAMP | Block timestamp |
| `block_number` | BIGINT | Block number |
| `tx_hash` | VARBINARY | Parent transaction hash |
| `tx_index` | BIGINT | Transaction index |
| `tx_success` | BOOLEAN | Parent tx success |
| `trace_address` | ARRAY(BIGINT) | Position in call tree |
| `type` | VARCHAR | Call type (call, delegatecall, create, etc.) |
| `from` | VARBINARY | Caller address |
| `to` | VARBINARY | Called address |
| `value` | UINT256 | Value in wei |
| `gas` | BIGINT | Gas provided |
| `gas_used` | BIGINT | Gas used |
| `input` | VARBINARY | Input data |
| `output` | VARBINARY | Return data |
| `success` | BOOLEAN | Whether call succeeded |
| `error` | VARCHAR | Error message if failed |
| `sub_traces` | BIGINT | Number of child traces |

### ethereum.logs

Event logs emitted by contracts.

| Column | Type | Description |
|--------|------|-------------|
| `block_time` | TIMESTAMP | Block timestamp |
| `block_number` | BIGINT | Block number |
| `block_hash` | VARBINARY | Block hash |
| `tx_hash` | VARBINARY | Transaction hash |
| `tx_index` | BIGINT | Transaction index |
| `index` | BIGINT | Log index in block |
| `contract_address` | VARBINARY | Emitting contract |
| `topic0` | VARBINARY | Event signature hash |
| `topic1` | VARBINARY | Indexed param 1 |
| `topic2` | VARBINARY | Indexed param 2 |
| `topic3` | VARBINARY | Indexed param 3 |
| `data` | VARBINARY | Non-indexed event data |

**Common Event Signatures (topic0):**
```sql
-- ERC20 Transfer
0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef

-- ERC20 Approval
0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925

-- ERC721 Transfer (same as ERC20)
0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
```

### ethereum.blocks

Block-level data.

| Column | Type | Description |
|--------|------|-------------|
| `time` | TIMESTAMP | Block timestamp |
| `number` | BIGINT | Block number |
| `hash` | VARBINARY | Block hash |
| `parent_hash` | VARBINARY | Parent block hash |
| `gas_limit` | BIGINT | Block gas limit |
| `gas_used` | BIGINT | Total gas used |
| `miner` | VARBINARY | Block producer address |
| `difficulty` | UINT256 | Block difficulty (pre-merge) |
| `total_difficulty` | UINT256 | Chain total difficulty |
| `size` | BIGINT | Block size in bytes |
| `base_fee_per_gas` | BIGINT | EIP-1559 base fee |
| `nonce` | VARBINARY | Block nonce |

### ethereum.contracts

Contract creation records.

| Column | Type | Description |
|--------|------|-------------|
| `address` | VARBINARY | Contract address |
| `bytecode` | VARBINARY | Deployed bytecode |
| `from` | VARBINARY | Deployer address |
| `created_at` | TIMESTAMP | Creation timestamp |
| `block_number` | BIGINT | Creation block |
| `tx_hash` | VARBINARY | Creation transaction |

---

## Bitcoin Raw Tables

### bitcoin.transactions

All Bitcoin transactions.

| Column | Type | Description |
|--------|------|-------------|
| `block_time` | TIMESTAMP | Block timestamp |
| `block_height` | BIGINT | Block height |
| `block_hash` | VARCHAR | Block hash |
| `id` | VARCHAR | Transaction ID (txid) |
| `index` | BIGINT | Index within block |
| `input_count` | BIGINT | Number of inputs |
| `output_count` | BIGINT | Number of outputs |
| `input_value` | DOUBLE | Total input value (BTC) |
| `output_value` | DOUBLE | Total output value (BTC) |
| `fee` | DOUBLE | Transaction fee (BTC) |
| `size` | BIGINT | Transaction size (bytes) |
| `virtual_size` | BIGINT | Virtual size (vbytes) |
| `is_coinbase` | BOOLEAN | Is coinbase transaction |
| `lock_time` | BIGINT | Transaction locktime |
| `hex` | VARCHAR | Raw transaction hex |

### bitcoin.inputs

Transaction inputs (UTXOs being spent).

| Column | Type | Description |
|--------|------|-------------|
| `block_time` | TIMESTAMP | Block timestamp (when input was spent) |
| `block_date` | DATE | Block date |
| `block_height` | BIGINT | Block height (when input was spent) |
| `block_hash` | VARBINARY | Block hash |
| `tx_id` | VARBINARY | Transaction ID |
| `index` | BIGINT | Input index within transaction |
| `spent_block_height` | BIGINT | Block height where the original UTXO was created |
| `spent_tx_id` | VARBINARY | TXID of the output being spent |
| `spent_output_number` | BIGINT | Index of the output being spent (note: not `spent_output_index`) |
| `value` | DOUBLE | Value in **BTC** (not satoshis) |
| `address` | VARCHAR | Spending address |
| `type` | VARCHAR | Script type (pubkeyhash, witness_v0_scripthash, witness_v1_taproot, etc.) |
| `coinbase` | VARBINARY | Coinbase data (for coinbase inputs only) |
| `is_coinbase` | BOOLEAN | Is coinbase input |
| `script_asm` | VARCHAR | Script in ASM format |
| `script_hex` | VARBINARY | Script in hex format |
| `script_desc` | VARCHAR | Script descriptor |
| `script_signature_asm` | VARCHAR | Signature script ASM |
| `script_signature_hex` | VARBINARY | Signature script hex |
| `sequence` | BIGINT | Sequence number |
| `witness_data` | VARBINARY | SegWit witness data |

**Important Notes:**
- `value` is in **BTC**, not satoshis (e.g., 0.31934359 BTC)
- `spent_block_height` enables BDD (Bitcoin Days Destroyed) calculation without joins
- Use `spent_output_number` (not `spent_output_index`) to reference the original output

### bitcoin.outputs

Transaction outputs (UTXOs created).

| Column | Type | Description |
|--------|------|-------------|
| `block_time` | TIMESTAMP | Block timestamp |
| `block_date` | DATE | Block date |
| `block_height` | BIGINT | Block height |
| `block_hash` | VARBINARY | Block hash |
| `tx_id` | VARBINARY | Transaction ID |
| `index` | BIGINT | Output index |
| `value` | DOUBLE | Value in **BTC** (not satoshis) |
| `address` | VARCHAR | Recipient address |
| `type` | VARCHAR | Script type (pubkeyhash, witness_v0_keyhash, nulldata, etc.) |
| `script_asm` | VARCHAR | Script in ASM format |
| `script_hex` | VARBINARY | Script in hex format |
| `script_desc` | VARCHAR | Script descriptor |

**Important Notes:**
- `value` is in **BTC**, not satoshis (e.g., 0.12081057 BTC)
- Columns `is_spent`, `spending_tx_id`, `spending_input_index` do **NOT** exist
- To track spending, use `bitcoin.inputs.spent_tx_id` and `spent_output_number` instead

### bitcoin.blocks

Bitcoin block data.

| Column | Type | Description |
|--------|------|-------------|
| `time` | TIMESTAMP | Block timestamp |
| `height` | BIGINT | Block height |
| `hash` | VARCHAR | Block hash |
| `previous_block_hash` | VARCHAR | Previous block hash |
| `transaction_count` | BIGINT | Number of transactions |
| `size` | BIGINT | Block size (bytes) |
| `stripped_size` | BIGINT | Size without witness |
| `weight` | BIGINT | Block weight |
| `difficulty` | DOUBLE | Mining difficulty |
| `nonce` | BIGINT | Block nonce |
| `coinbase_value` | BIGINT | Block reward (satoshis) |

---

## Other EVM Chains

The following chains have the same table structure as Ethereum:

| Chain | Schema Prefix | Native Token Decimals |
|-------|---------------|----------------------|
| Polygon | `polygon.` | 18 (MATIC) |
| Arbitrum | `arbitrum.` | 18 (ETH) |
| Optimism | `optimism.` | 18 (ETH) |
| Base | `base.` | 18 (ETH) |
| BNB Chain | `bnb.` | 18 (BNB) |
| Avalanche C-Chain | `avalanche_c.` | 18 (AVAX) |
| Fantom | `fantom.` | 18 (FTM) |
| Gnosis | `gnosis.` | 18 (xDAI) |
| zkSync Era | `zksync.` | 18 (ETH) |
| Linea | `linea.` | 18 (ETH) |
| Scroll | `scroll.` | 18 (ETH) |
| Celo | `celo.` | 18 (CELO) |

**Example:**
```sql
-- Polygon transactions
SELECT * FROM polygon.transactions WHERE block_time >= DATE '2024-01-01'

-- Arbitrum logs
SELECT * FROM arbitrum.logs WHERE topic0 = 0x...
```

---

## Token Metadata

### tokens.erc20

ERC20 token information.

| Column | Type | Description |
|--------|------|-------------|
| `blockchain` | VARCHAR | Chain name |
| `contract_address` | VARBINARY | Token contract address |
| `symbol` | VARCHAR | Token symbol |
| `decimals` | BIGINT | Token decimals |

**Example:**
```sql
SELECT
    t.symbol,
    t.decimals,
    tr.value / POWER(10, t.decimals) AS amount
FROM ethereum.transactions tr
JOIN tokens.erc20 t ON t.contract_address = tr.to
WHERE t.blockchain = 'ethereum'
```

### tokens.nft

NFT collection metadata.

| Column | Type | Description |
|--------|------|-------------|
| `blockchain` | VARCHAR | Chain name |
| `contract_address` | VARBINARY | Collection address |
| `name` | VARCHAR | Collection name |
| `symbol` | VARCHAR | Collection symbol |
| `standard` | VARCHAR | Token standard (erc721, erc1155) |

---

## Price Tables

### prices.usd

Historical token prices in USD.

| Column | Type | Description |
|--------|------|-------------|
| `minute` | TIMESTAMP | Price timestamp (minute granularity) |
| `blockchain` | VARCHAR | Chain name (or NULL for native) |
| `contract_address` | VARBINARY | Token contract (or NULL for native) |
| `symbol` | VARCHAR | Token symbol |
| `price` | DOUBLE | USD price |
| `decimals` | BIGINT | Token decimals |

**Common Price Lookups:**
```sql
-- ETH price
SELECT * FROM prices.usd
WHERE symbol = 'ETH' AND blockchain IS NULL
  AND minute >= DATE '2024-01-01'

-- USDC price on Ethereum
SELECT * FROM prices.usd
WHERE symbol = 'USDC' AND blockchain = 'ethereum'
  AND minute >= DATE '2024-01-01'
```

### prices.usd_latest

Current token prices (faster for latest prices).

| Column | Type | Description |
|--------|------|-------------|
| `blockchain` | VARCHAR | Chain name |
| `contract_address` | VARBINARY | Token contract |
| `symbol` | VARCHAR | Token symbol |
| `price` | DOUBLE | Current USD price |
| `decimals` | BIGINT | Token decimals |

---

## Spellbook Tables

Spellbook tables are curated, cross-protocol aggregations maintained by the Dune community.

### dex.trades

Unified DEX trades across all protocols.

| Column | Type | Description |
|--------|------|-------------|
| `blockchain` | VARCHAR | Chain name |
| `project` | VARCHAR | Protocol name (uniswap, sushiswap, etc.) |
| `version` | VARCHAR | Protocol version |
| `block_time` | TIMESTAMP | Trade timestamp |
| `block_date` | DATE | Trade date |
| `block_number` | BIGINT | Block number |
| `tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Event index |
| `token_bought_address` | VARBINARY | Bought token contract |
| `token_bought_symbol` | VARCHAR | Bought token symbol |
| `token_bought_amount` | DOUBLE | Amount bought (decimal adjusted) |
| `token_bought_amount_raw` | UINT256 | Raw amount bought |
| `token_sold_address` | VARBINARY | Sold token contract |
| `token_sold_symbol` | VARCHAR | Sold token symbol |
| `token_sold_amount` | DOUBLE | Amount sold (decimal adjusted) |
| `token_sold_amount_raw` | UINT256 | Raw amount sold |
| `amount_usd` | DOUBLE | Trade value in USD |
| `taker` | VARBINARY | Trade taker address |
| `maker` | VARBINARY | Trade maker address |
| `project_contract_address` | VARBINARY | Pool/pair contract |
| `tx_from` | VARBINARY | Transaction sender |
| `tx_to` | VARBINARY | Transaction recipient |

**Example:**
```sql
SELECT
    date_trunc('day', block_time) AS day,
    project,
    SUM(amount_usd) AS volume_usd
FROM dex.trades
WHERE blockchain = 'ethereum'
  AND block_time >= DATE '2024-01-01'
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
```

### nft.trades

Unified NFT trades across marketplaces.

| Column | Type | Description |
|--------|------|-------------|
| `blockchain` | VARCHAR | Chain name |
| `project` | VARCHAR | Marketplace (opensea, blur, etc.) |
| `version` | VARCHAR | Protocol version |
| `block_time` | TIMESTAMP | Trade timestamp |
| `block_date` | DATE | Trade date |
| `block_number` | BIGINT | Block number |
| `tx_hash` | VARBINARY | Transaction hash |
| `nft_contract_address` | VARBINARY | NFT collection address |
| `token_id` | UINT256 | NFT token ID |
| `token_standard` | VARCHAR | Token standard |
| `trade_type` | VARCHAR | Trade type (single, bundle) |
| `number_of_items` | BIGINT | Items in trade |
| `trade_category` | VARCHAR | Category (buy, sell) |
| `buyer` | VARBINARY | Buyer address |
| `seller` | VARBINARY | Seller address |
| `currency_contract` | VARBINARY | Payment token |
| `currency_symbol` | VARCHAR | Payment symbol |
| `amount_original` | DOUBLE | Original amount |
| `amount_usd` | DOUBLE | USD value |
| `platform_fee_amount_raw` | UINT256 | Platform fee (raw) |
| `platform_fee_amount` | DOUBLE | Platform fee |
| `platform_fee_amount_usd` | DOUBLE | Platform fee USD |
| `royalty_fee_amount_raw` | UINT256 | Royalty fee (raw) |
| `royalty_fee_amount` | DOUBLE | Royalty fee |
| `royalty_fee_amount_usd` | DOUBLE | Royalty fee USD |

### transfers.erc20

ERC20 token transfers.

| Column | Type | Description |
|--------|------|-------------|
| `blockchain` | VARCHAR | Chain name |
| `block_time` | TIMESTAMP | Transfer timestamp |
| `block_date` | DATE | Transfer date |
| `block_number` | BIGINT | Block number |
| `tx_hash` | VARBINARY | Transaction hash |
| `evt_index` | BIGINT | Event index |
| `contract_address` | VARBINARY | Token contract |
| `from` | VARBINARY | Sender address |
| `to` | VARBINARY | Recipient address |
| `amount_raw` | UINT256 | Raw transfer amount |
| `amount` | DOUBLE | Decimal-adjusted amount |

### balances.erc20_latest

Latest ERC20 token balances by address.

| Column | Type | Description |
|--------|------|-------------|
| `blockchain` | VARCHAR | Chain name |
| `address` | VARBINARY | Wallet address |
| `token_address` | VARBINARY | Token contract |
| `token_symbol` | VARCHAR | Token symbol |
| `amount` | DOUBLE | Current balance |
| `amount_raw` | UINT256 | Raw balance |
| `amount_usd` | DOUBLE | USD value |

### labels.addresses

Community-maintained address labels.

| Column | Type | Description |
|--------|------|-------------|
| `blockchain` | VARCHAR | Chain name |
| `address` | VARBINARY | Labeled address |
| `name` | VARCHAR | Label name |
| `category` | VARCHAR | Label category |
| `contributor` | VARCHAR | Label contributor |
| `source` | VARCHAR | Label source |
| `created_at` | TIMESTAMP | Label creation time |
| `updated_at` | TIMESTAMP | Label update time |

---

## Common Decoded Tables

Decoded tables have protocol-specific schemas. Common naming pattern:

```
{protocol}_{version}_{chain}.{Contract}_{evt/call}_{EventOrFunction}
```

### Uniswap V3

```sql
-- Swap events
uniswap_v3_ethereum.Pair_evt_Swap

| Column | Type | Description |
|--------|------|-------------|
| evt_block_time | TIMESTAMP | Event timestamp |
| evt_block_number | BIGINT | Block number |
| evt_tx_hash | VARBINARY | Transaction hash |
| evt_index | BIGINT | Event index |
| contract_address | VARBINARY | Pool address |
| sender | VARBINARY | Swap initiator |
| recipient | VARBINARY | Output recipient |
| amount0 | INT256 | Token0 amount delta |
| amount1 | INT256 | Token1 amount delta |
| sqrtPriceX96 | UINT256 | New sqrt price |
| liquidity | UINT128 | Pool liquidity |
| tick | INT256 | New tick |
```

### Aave V3

```sql
-- Supply events
aave_v3_ethereum.Pool_evt_Supply

| Column | Type | Description |
|--------|------|-------------|
| evt_block_time | TIMESTAMP | Event timestamp |
| evt_tx_hash | VARBINARY | Transaction hash |
| contract_address | VARBINARY | Pool address |
| reserve | VARBINARY | Asset address |
| user | VARBINARY | User address |
| onBehalfOf | VARBINARY | Recipient |
| amount | UINT256 | Supplied amount |
| referralCode | UINT16 | Referral code |
```

### OpenSea Seaport

```sql
-- Order fulfillment events
seaport_ethereum.Seaport_evt_OrderFulfilled

| Column | Type | Description |
|--------|------|-------------|
| evt_block_time | TIMESTAMP | Event timestamp |
| evt_tx_hash | VARBINARY | Transaction hash |
| orderHash | VARBINARY | Order hash |
| offerer | VARBINARY | Seller address |
| recipient | VARBINARY | Buyer address |
| zone | VARBINARY | Zone address |
| offer | ARRAY | Offered items |
| consideration | ARRAY | Consideration items |
```

---

## Useful Functions

### Type Conversions

```sql
-- Address to string (for display)
LOWER(CAST(address AS VARCHAR))

-- Hex string to address
FROM_HEX('0x...')

-- Integer to varbinary
CAST(value AS VARBINARY)

-- Extract address from bytes32 (right-aligned)
CAST(SUBSTRING(topic1, 13, 20) AS VARBINARY)
```

### Date/Time Functions

```sql
-- Truncate to day
date_trunc('day', block_time)

-- Truncate to hour
date_trunc('hour', block_time)

-- Date arithmetic
block_time + INTERVAL '7' DAY
block_time - INTERVAL '1' HOUR

-- Current date/time
CURRENT_DATE
CURRENT_TIMESTAMP
NOW()
```

### Numeric Functions

```sql
-- Decimal adjustment (ETH has 18 decimals)
value / 1e18

-- Power function for dynamic decimals
value / POWER(10, decimals)

-- Percentiles
APPROX_PERCENTILE(value, 0.5)  -- median
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value)  -- exact median

-- Rounding
ROUND(value, 2)
FLOOR(value)
CEIL(value)
```

### String Functions

```sql
-- Concatenation
CONCAT('0x', TO_HEX(address))

-- Substring
SUBSTRING(data, 1, 4)  -- first 4 bytes

-- Lower/upper case
LOWER(symbol)
UPPER(symbol)
```

### Aggregations

```sql
-- Basic aggregations
COUNT(*)
COUNT(DISTINCT address)
SUM(value)
AVG(value)
MIN(value)
MAX(value)

-- Array aggregation
ARRAY_AGG(DISTINCT address)

-- Conditional aggregation
SUM(CASE WHEN success THEN value ELSE 0 END)
COUNT_IF(success)
SUM_IF(value, success)
```

### Window Functions

```sql
-- Running total
SUM(value) OVER (ORDER BY block_time)

-- Rank
ROW_NUMBER() OVER (PARTITION BY address ORDER BY block_time DESC)

-- Lead/lag
LAG(value, 1) OVER (ORDER BY block_time)
LEAD(value, 1) OVER (ORDER BY block_time)
```

---

## Notes

- **Address Format**: Always use lowercase for consistency
- **Value Decimals**: Remember to divide by `10^decimals` for human-readable amounts
- **Date Filters**: Always include date filters on large tables to control query costs
- **Block Time vs Date**: Use `block_time` for precise timestamps, `block_date` for daily aggregations
- **Spellbook vs Raw**: Prefer Spellbook tables when available for cleaner, pre-processed data

---

## See Also

- [CLAUDE.md](../CLAUDE.md) - Development guidelines and query conventions
- [Dune Documentation](https://docs.dune.com/)
- [Dune Spellbook Repository](https://github.com/duneanalytics/spellbook)
- [Trino SQL Reference](https://trino.io/docs/current/)
