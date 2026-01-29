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

## See Also

- [README.md](./README.md) - Contributing guidelines and query documentation template
- [CLAUDE.md](../CLAUDE.md) - Development guidelines and query conventions
- [dune_database_schemas.md](./dune_database_schemas.md) - Dune table schemas reference
