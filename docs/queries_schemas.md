# Repository Queries Documentation

This document provides an overview of all SQL queries in the `/queries/` directory, including their purpose, input/output schemas, and dependencies.

> **Last Updated:** 2026-01-29

---

## Table of Contents

- [Overview](#overview)
- [Query Index](#query-index)
- [Bitcoin Queries](#bitcoin-queries)
  - [bitcoin_intent_heuristics.sql](#bitcoin_intent_heuristicssql)
  - [bitcoin_privacy_heuristics_v2.sql](#bitcoin_privacy_heuristics_v2sql)

---

## Overview

This repository organizes queries by blockchain:

```
queries/
├── bitcoin/           # Bitcoin network queries
├── ethereum/          # Ethereum mainnet queries (planned)
├── polygon/           # Polygon queries (planned)
├── arbitrum/          # Arbitrum queries (planned)
└── cross-chain/       # Multi-chain queries (planned)
```

### Query Naming Convention

- `{analysis_type}.sql` - General analysis queries
- `{protocol}_{analysis}.sql` - Protocol-specific queries
- `{metric}_daily.sql` - Daily aggregation queries

---

## Query Index

| Query | Blockchain | Description | Tables Used |
|-------|------------|-------------|-------------|
| [bitcoin_intent_heuristics.sql](#bitcoin_intent_heuristicssql) | Bitcoin | Classifies transactions by intent patterns | `bitcoin.inputs`, `bitcoin.outputs` |
| [bitcoin_privacy_heuristics_v2.sql](#bitcoin_privacy_heuristics_v2sql) | Bitcoin | Detects privacy issues using Esplora-style heuristics | `bitcoin.inputs`, `bitcoin.outputs` |

---

## Bitcoin Queries

### bitcoin_intent_heuristics.sql

**Path:** `queries/bitcoin/bitcoin_intent_heuristics.sql`

**Description:**
Classifies Bitcoin transactions by their likely intent based on input/output patterns. Uses heuristics to identify consolidations, batch payments, CoinJoin-like transactions, and other common patterns.

**Author:** stefanopepe
**Created:** 2026-01-28

#### Purpose

Helps analysts understand the composition of Bitcoin transaction activity by categorizing transactions into behavioral intents rather than just raw counts.

#### Dune Tables Used

| Table | Purpose | Key Columns Used |
|-------|---------|------------------|
| `bitcoin.inputs` | Source of transaction inputs | `block_time`, `tx_id`, `value`, `is_coinbase` |
| `bitcoin.outputs` | Transaction outputs | `block_time`, `tx_id`, `value` |

#### Input Parameters

This query uses Dune's incremental processing with `previous.query.result()`. No user-defined parameters required.

**Incremental Processing:**
- Uses 1-day lookback window for recomputation
- Default fallback date: `2026-01-01` (adjustable in `checkpoint` CTE)
- Excludes coinbase transactions from analysis

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `day` | DATE | Date of transactions |
| `intent` | VARCHAR | Classified transaction intent category |
| `tx_count` | BIGINT | Number of transactions in category |
| `sats_in` | DOUBLE | Total input value (satoshis) |
| `sats_out` | DOUBLE | Total output value (satoshis) |
| `avg_inputs` | DOUBLE | Average input count per transaction |
| `avg_outputs` | DOUBLE | Average output count per transaction |
| `median_inputs` | DOUBLE | Median input count per transaction |
| `median_outputs` | DOUBLE | Median output count per transaction |

#### Intent Classification Logic

| Intent | Criteria | Description |
|--------|----------|-------------|
| `consolidation` | inputs ≥ 10, outputs ≤ 2 | Combining many UTXOs into few |
| `fan_out_batch` | inputs ≤ 2, outputs ≥ 10 | Batch payments (exchanges, payroll) |
| `coinjoin_like` | inputs ≥ 5, outputs ≥ 5, \|inputs - outputs\| ≤ 1 | Privacy-enhancing transactions |
| `self_transfer` | inputs = 1, outputs = 1 | Single input to single output |
| `change_like_2_outputs` | inputs ≥ 2, outputs = 2 | Standard payment with change |
| `malformed_no_outputs` | outputs = 0 | Edge case: no outputs |
| `other` | All other patterns | Unclassified transactions |

#### Example Output

```
| day        | intent               | tx_count | sats_in        | sats_out       | avg_inputs | avg_outputs |
|------------|----------------------|----------|----------------|----------------|------------|-------------|
| 2026-01-27 | change_like_2_outputs| 245832   | 1234567890123  | 1234567890000  | 2.4        | 2.0         |
| 2026-01-27 | consolidation        | 12543    | 987654321098   | 987654321000   | 15.2       | 1.3         |
| 2026-01-27 | fan_out_batch        | 8921     | 567890123456   | 567890123400   | 1.5        | 45.7        |
| 2026-01-27 | coinjoin_like        | 1234     | 123456789012   | 123456789000   | 52.3       | 52.1        |
| 2026-01-27 | self_transfer        | 98765    | 345678901234   | 345678901200   | 1.0        | 1.0         |
| 2026-01-27 | other                | 156789   | 456789012345   | 456789012300   | 3.2        | 4.8         |
```

#### Query Structure

```
┌─────────────────────────────────────────────────────────────────────┐
│ CTE: prev                                                           │
│   Load previous query results (incremental processing)              │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ CTE: checkpoint                                                     │
│   Calculate cutoff date (max(prev.day) - 1 day)                     │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ CTEs: inputs_by_tx, outputs_by_tx                                   │
│   Aggregate inputs/outputs per transaction for new date range       │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ CTE: tx                                                             │
│   Join inputs and outputs at transaction level                      │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ CTE: classified                                                     │
│   Apply intent classification rules                                 │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ CTE: new_data                                                       │
│   Aggregate classified transactions by day and intent               │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Final: UNION kept_old + new_data                                    │
│   Merge historical data with newly computed data                    │
└─────────────────────────────────────────────────────────────────────┘
```

#### Notes

- **Coinbase Exclusion:** Coinbase transactions (block rewards) are excluded since they don't represent user-initiated activity
- **Incremental Design:** Query is designed for efficient incremental updates; only recomputes recent data
- **Thresholds:** The input/output count thresholds (10, 5, 2, 1) are configurable heuristics based on common Bitcoin usage patterns

---

### bitcoin_privacy_heuristics_v2.sql

**Path:** `queries/bitcoin/bitcoin_privacy_heuristics_v2.sql`

**Description:**
Implements advanced privacy analysis heuristics based on Blockstream Esplora's privacy-analysis.js methodology. Detects change outputs, unnecessary inputs, CoinJoin patterns, self-transfers, and address reuse.

**Author:** stefanopepe
**Created:** 2026-01-29
**Reference:** [Esplora privacy-analysis.js](https://github.com/Blockstream/esplora/blob/master/client/src/lib/privacy-analysis.js)

#### Purpose

Helps privacy researchers and analysts identify transactions with potential privacy leaks. Based on well-established heuristics from Blockstream's Esplora block explorer, this query flags transactions where:
- Change outputs can be identified through precision or script type analysis
- Inputs are unnecessarily included (revealing wallet composition)
- CoinJoin mixing patterns are detected
- Address reuse occurs within the same transaction

#### Dune Tables Used

| Table | Purpose | Key Columns Used |
|-------|---------|------------------|
| `bitcoin.inputs` | Transaction inputs with script types | `block_time`, `tx_id`, `value`, `address`, `type`, `is_coinbase` |
| `bitcoin.outputs` | Transaction outputs with script types | `block_time`, `tx_id`, `value`, `address`, `type` |

#### Input Parameters

This query uses Dune's incremental processing with `previous.query.result()`. No user-defined parameters required.

**Incremental Processing:**
- Uses 1-day lookback window for recomputation
- Default fallback date: `2026-01-01` (adjustable in `checkpoint` CTE)
- Excludes coinbase transactions from analysis

#### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `day` | DATE | Date of transactions |
| `privacy_heuristic` | VARCHAR | The privacy issue detected |
| `tx_count` | BIGINT | Number of transactions with this issue |
| `sats_total` | DOUBLE | Total satoshis involved |
| `avg_inputs` | DOUBLE | Average input count per transaction |
| `avg_outputs` | DOUBLE | Average output count per transaction |

#### Privacy Heuristic Classification

| Heuristic | Criteria | Description |
|-----------|----------|-------------|
| `change_precision` | 2 outputs with ≥3 digit precision difference | Round payment amount vs. change with many decimal places |
| `change_script_type` | 2 outputs with different script types, one matches inputs | Output type mismatch reveals which is change |
| `uih1` | Smallest input covers smallest output + fee | Unnecessary Input Heuristic 1: smallest output is likely change |
| `uih2` | Smallest input covers largest output + fee | Unnecessary Input Heuristic 2: exotic transaction motive |
| `coinjoin_detected` | ≥50% equal outputs, 2-5+ matching, multiple inputs | CoinJoin mixing pattern detected |
| `self_transfer` | Single output (no change) | Wallet consolidation, exchange transfer, or channel funding |
| `address_reuse` | Output address matches an input address | Internal address reuse within transaction |
| `no_privacy_issues` | No heuristics triggered | Transaction passes all privacy checks |

#### Heuristic Details

**Precision-Based Change Detection:**
Compares trailing zeros in satoshi values of two outputs. A difference of ≥3 digits suggests one output is a round payment (e.g., 100000000 sats = 1 BTC) while the other is precise change (e.g., 12345678 sats).

**Script Type Mismatch:**
When two outputs have different script types (e.g., P2PKH vs P2WPKH) but inputs only use one type, the mismatched output is likely change sent to a new address type.

**Unnecessary Input Heuristic (UIH):**
If the smallest input could be removed and the transaction would still have enough to cover an output + fee, it suggests:
- UIH1: The smallest output is change (not the payment)
- UIH2: The transaction has exotic motives (paying to yourself, etc.)

UIH checks are skipped when all inputs have the same script type (privacy-preserving consolidation pattern).

**CoinJoin Detection:**
Identifies transactions where ≥50% of outputs share identical values, with at least 2-5 matching outputs. Requires multiple inputs to distinguish from normal batched payments.

#### Example Output

```
| day        | privacy_heuristic   | tx_count | sats_total      | avg_inputs | avg_outputs |
|------------|---------------------|----------|-----------------|------------|-------------|
| 2026-01-28 | no_privacy_issues   | 312456   | 8765432109876   | 2.1        | 2.8         |
| 2026-01-28 | change_precision    | 89234    | 2345678901234   | 2.3        | 2.0         |
| 2026-01-28 | self_transfer       | 45678    | 1234567890123   | 1.8        | 1.0         |
| 2026-01-28 | change_script_type  | 23456    | 567890123456    | 2.1        | 2.0         |
| 2026-01-28 | coinjoin_detected   | 1234     | 987654321098    | 48.5       | 50.2        |
| 2026-01-28 | address_reuse       | 5678     | 123456789012    | 3.2        | 4.1         |
| 2026-01-28 | uih1                | 4567     | 234567890123    | 3.8        | 2.0         |
| 2026-01-28 | uih2                | 1234     | 98765432109     | 4.2        | 2.0         |
```

#### Query Structure

```
┌─────────────────────────────────────────────────────────────────────┐
│ CTEs: prev, checkpoint                                              │
│   Incremental processing setup                                      │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ CTEs: raw_inputs, raw_outputs                                       │
│   Get detailed input/output data with addresses and script types    │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ CTEs: tx_input_stats, tx_output_stats                               │
│   Aggregate stats per transaction (counts, min/max values, types)   │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ CTEs: two_output_details, coinjoin_detection, address_reuse         │
│   Specialized detections for specific heuristics                    │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ CTE: tx_combined, tx_with_precision                                 │
│   Combine all data and calculate precision metrics                  │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ CTE: classified                                                     │
│   Apply privacy heuristic classification (priority order)           │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Final: UNION kept_old + new_data                                    │
│   Merge historical data with newly computed data                    │
└─────────────────────────────────────────────────────────────────────┘
```

#### Notes

- **Heuristic Priority:** Heuristics are evaluated in order; a transaction is classified by the first matching heuristic
- **UIH Exclusion:** UIH checks skip transactions where all inputs share the same script type (legitimate consolidation)
- **CoinJoin Threshold:** Requires ≥50% of outputs to be equal, capped between 2-5 matching outputs
- **Precision Calculation:** Uses trailing zeros in satoshi values to estimate "roundness"
- **Incremental Design:** Designed for efficient daily updates with 1-day lookback recomputation

---

## Adding New Queries

When adding a new query to this repository:

1. **Create the SQL file** in the appropriate blockchain directory
2. **Include the standard header** (see [CLAUDE.md](../CLAUDE.md) for format)
3. **Validate against schemas** in [`dune_database_schemas.md`](./dune_database_schemas.md)
4. **Document in this file** following the template below

### Query Documentation Template

```markdown
### query_name.sql

**Path:** `queries/{blockchain}/query_name.sql`

**Description:**
[Brief description of what the query does]

**Author:** [username]
**Created:** [YYYY-MM-DD]

#### Purpose
[Why this query is useful]

#### Dune Tables Used
| Table | Purpose | Key Columns Used |
|-------|---------|------------------|
| `table.name` | [purpose] | `col1`, `col2` |

#### Input Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `{{param}}` | TYPE | value | [description] |

#### Output Schema
| Column | Type | Description |
|--------|------|-------------|
| `column_name` | TYPE | [description] |

#### Notes
[Any special considerations, limitations, or usage notes]
```

---

## See Also

- [CLAUDE.md](../CLAUDE.md) - Development guidelines and query conventions
- [dune_database_schemas.md](./dune_database_schemas.md) - Dune table schemas reference
