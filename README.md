# DuneQueries

A curated collection of SQL queries for [Dune Analytics](https://dune.com/), the blockchain analytics platform for querying on-chain data across Ethereum, Polygon, Arbitrum, Optimism, Solana, and more.

## Overview

This repository provides:

- **Version-controlled queries** for blockchain data analysis
- **Reusable templates** and query patterns
- **Documented techniques** for on-chain analytics
- **Production-ready queries** tested against live data

## Development Pipeline

We follow a structured 4-step pipeline for developing and validating Dune queries:

```
Schema Exploration → Query Development → Validation → Cost Estimation
```

### 1. Schema Exploration

Use the Dune API to explore table schemas and data formats before writing queries.

```bash
export DUNE_API_KEY="your_api_key_here"

# Execute and fetch query results
curl -X GET "https://api.dune.com/api/v1/query/QUERY_ID/results" \
  -H "X-Dune-API-Key: $DUNE_API_KEY"
```

### 2. Query Development

Write and refine SQL queries with focus on:
- Logical correctness and accurate table/column references
- Style compliance and optimization
- Proper documentation and parameterization

### 3. Validation

Run smoke tests to catch:
- Non-existent tables, columns, or functions
- Syntax and logic errors
- Performance issues

### 4. Cost Estimation

Estimate computational footprint before running expensive queries by checking row counts and understanding query plans.

## Repository Structure

```
DuneQueries/
├── .env.example           # Example environment variables (DUNE_API_KEY)
├── queries/               # Main query directory
│   ├── ethereum/          # Ethereum mainnet queries
│   │   ├── defi/          # DeFi protocol queries
│   │   ├── nft/           # NFT marketplace queries
│   │   ├── tokens/        # Token analysis queries
│   │   └── wallets/       # Wallet analysis queries
│   ├── polygon/           # Polygon queries
│   ├── arbitrum/          # Arbitrum queries
│   ├── optimism/          # Optimism queries
│   ├── solana/            # Solana queries
│   └── cross-chain/       # Multi-chain queries
├── templates/             # Reusable query templates
├── tests/                 # Validation and smoke tests
│   ├── smoke/             # Quick validation scripts
│   └── schemas/           # Expected schema definitions
├── scripts/               # Utility scripts
│   ├── validate_query.sh  # Query validation script
│   └── estimate_cost.sh   # Cost estimation script
├── spells/                # Dune Spellbook contributions
└── docs/                  # Additional documentation
```

## Quick Start

### Example Query

```sql
-- Daily ETH transfer volume
SELECT
    date_trunc('day', block_time) AS day,
    SUM(value / 1e18) AS eth_volume,
    COUNT(*) AS tx_count
FROM ethereum.transactions
WHERE
    block_time >= DATE '{{start_date}}'
    AND block_time < DATE '{{end_date}}'
    AND value > 0
GROUP BY 1
ORDER BY 1 DESC
```

### Common Patterns

```sql
-- Convert wei to ETH
value / 1e18 AS eth_amount

-- Get USD value with price join
t.value / 1e18 * p.price AS usd_value

-- Time aggregations
date_trunc('day', block_time) AS day
```

## SQL Conventions

- **Keywords**: UPPERCASE (`SELECT`, `FROM`, `WHERE`)
- **Identifiers**: lowercase for tables and columns
- **Parameters**: Use `{{parameter_name}}` for configurable values
- **Comments**: Include header blocks with description, author, and dates

## Key Resources

| Resource | Link |
|----------|------|
| Dune Documentation | [docs.dune.com](https://docs.dune.com/) |
| Dune API | [API Reference](https://docs.dune.com/api-reference/overview) |
| Data Tables Reference | [Blockchain Data](https://docs.dune.com/data-tables/) |
| Spellbook | [GitHub](https://github.com/duneanalytics/spellbook) |
| Trino SQL | [Documentation](https://trino.io/docs/current/) |

## Contributing

1. Create queries in the appropriate directory
2. Include standard header comment blocks
3. Test on Dune Analytics before committing
4. Follow the SQL style guide in [CLAUDE.md](./CLAUDE.md)

For detailed AI assistant guidelines and comprehensive development instructions, see [CLAUDE.md](./CLAUDE.md).

## License

Apache License 2.0 - See [LICENSE](./LICENSE) for details.
