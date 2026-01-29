# CLAUDE.md - AI Assistant Guide for DuneQueries

This document provides comprehensive guidance for AI assistants working with this repository.

## Repository Overview

**DuneQueries** is a collection of SQL queries for [Dune Analytics](https://dune.com/), a blockchain analytics platform that allows users to query on-chain data from various blockchains (Ethereum, Polygon, Arbitrum, Optimism, Solana, etc.) using SQL.

### Purpose

- Store and version-control Dune Analytics SQL queries
- Share reusable query patterns and templates
- Document blockchain data analysis techniques
- Maintain a library of tested, production-ready queries

### License

Apache License 2.0 - See LICENSE file for details.

---

## Development Pipeline

This repository follows a structured pipeline for developing and validating Dune Analytics queries:

### Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  1. SCHEMA EXPLORATION                                                      │
│     Use Dune API (curl) to explore table schemas and data formats           │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  2. QUERY WRITING                                                           │
│     Write SQL queries directly in Claude Code                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  3. VALIDATION (DRY RUN / SMOKE TEST)                                       │
│     Test queries to catch errors and verify correctness                     │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  4. COST ESTIMATION (Nice to Have)                                          │
│     Estimate query computation footprint and Dune token costs               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 1: Schema Exploration via Dune API

Use the Dune API with curl to explore available tables, schemas, and data formats before writing queries.

> **IMPORTANT:** Before making API calls, consult [`docs/dune_database_schemas.md`](./docs/dune_database_schemas.md) for a reliable offline reference of table schemas. This avoids dependency on Dune's documentation endpoints which can be unreliable.

**Environment Setup:**
```bash
export DUNE_API_KEY="your_api_key_here"
```

**Example API Calls:**
```bash
# Execute a query to explore a table schema
curl -X POST "https://api.dune.com/api/v1/query/execute" \
  -H "X-Dune-API-Key: $DUNE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query_id": YOUR_QUERY_ID}'

# Get query results
curl -X GET "https://api.dune.com/api/v1/query/QUERY_ID/results" \
  -H "X-Dune-API-Key: $DUNE_API_KEY"

# Check execution status
curl -X GET "https://api.dune.com/api/v1/execution/EXECUTION_ID/status" \
  -H "X-Dune-API-Key: $DUNE_API_KEY"
```

**Schema Discovery Queries:**
```sql
-- List columns in a table
DESCRIBE ethereum.transactions

-- Sample data from a table
SELECT * FROM ethereum.transactions LIMIT 10

-- Check available tables in a schema
SHOW TABLES IN ethereum
```

### Step 2: Query Writing in Claude Code

Write SQL queries directly in Claude Code. The AI assistant will:

1. **Understand Requirements** - Interpret the user's data analysis needs
2. **Select Appropriate Tables** - Choose the right Dune tables based on schema exploration
3. **Write Optimized SQL** - Generate efficient Trino/Presto SQL queries
4. **Follow Conventions** - Apply repository style guide and header templates
5. **Add Documentation** - Include inline comments for complex logic
6. **Parameterize Values** - Use Dune parameters (`{{param}}`) for configurable values

**Best Practices for Query Requests:**
- Be specific about the metrics and dimensions you want
- Mention relevant tables if known (e.g., "using ethereum.transactions")
- Specify date ranges and filters
- Indicate the desired output format (daily aggregates, top N, etc.)

**Example Request:**
> "Calculate daily ETH transfer volume in USD for the last 30 days, grouped by day, using ethereum.transactions and prices.usd"

### Step 3: Validation (Dry Run / Smoke Test)

Before committing queries, run them through the validation environment to catch:

- **Hallucinations** - Non-existent tables, columns, or functions
- **Syntax Errors** - SQL that won't execute
- **Logic Errors** - Queries that run but produce incorrect results
- **Performance Issues** - Queries that timeout or consume excessive resources

#### Schema Validation Against Local Reference

**CRITICAL:** Before committing any query, validate all table and column references against [`docs/dune_database_schemas.md`](./docs/dune_database_schemas.md).

**AI Assistant Schema Validation Protocol:**
1. **Extract all table references** from the query (e.g., `bitcoin.inputs`, `ethereum.transactions`)
2. **Cross-check each table** exists in `docs/dune_database_schemas.md`
3. **Verify all column names** used in SELECT, WHERE, JOIN, GROUP BY clauses match documented schemas
4. **Check data types** are used correctly (e.g., VARBINARY for addresses, UINT256 for values)
5. **Validate functions** are compatible with Trino/Presto SQL dialect

**Common Hallucination Patterns to Catch:**
- Invented column names (e.g., `tx_value` instead of `value`, `timestamp` instead of `block_time`)
- Non-existent tables (e.g., `bitcoin.transactions.inputs` instead of `bitcoin.inputs`)
- Wrong schema prefixes (e.g., `btc.` instead of `bitcoin.`)
- Fabricated Spellbook tables (always verify against documented tables)
- Incorrect function names (e.g., `TO_TIMESTAMP` instead of `date_trunc`)

**Validation Checklist:**
- [ ] Query executes without syntax errors
- [ ] All referenced tables exist in `docs/dune_database_schemas.md`
- [ ] All referenced columns exist in their documented table schemas
- [ ] Data types match (addresses as VARBINARY, values as appropriate numeric types)
- [ ] Results are non-empty (unless expected)
- [ ] Results are reasonable (sanity check values)
- [ ] Query completes within acceptable time

**Test Environment Location:** `tests/` directory (to be implemented)

### Step 4: Cost Estimation (Nice to Have)

Estimate the computational footprint before running expensive queries:

**Factors Affecting Cost:**
- Data scanned (rows and columns)
- Time range covered
- Number of JOINs
- Aggregation complexity
- Result set size

**Estimation Approaches:**
```sql
-- Check row count for a time range
SELECT COUNT(*) FROM ethereum.transactions
WHERE block_time >= DATE '2024-01-01' AND block_time < DATE '2024-02-01'

-- Use EXPLAIN to understand query plan (when available)
EXPLAIN SELECT ...
```

**Dune Credits Reference:**
- Credits consumed based on query execution time and data processed
- Monitor usage in Dune dashboard
- Consider query caching for repeated executions

---

## Directory Structure (Recommended)

When adding content to this repository, follow this structure:

```
DuneQueries/
├── CLAUDE.md              # This file - AI assistant guidelines
├── README.md              # Project overview and usage instructions
├── LICENSE                # Apache 2.0 License
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
├── tests/                 # Validation and smoke tests
│   ├── smoke/             # Quick validation scripts
│   ├── schemas/           # Expected schema definitions
│   └── README.md          # Test environment documentation
├── scripts/               # Utility scripts
│   ├── validate_query.sh  # Query validation script
│   └── estimate_cost.sh   # Cost estimation script
├── templates/             # Reusable query templates
├── spells/                # Dune Spellbook contributions
└── docs/                  # Additional documentation
    ├── dune_database_schemas.md  # Dune table schemas reference
    └── queries_schemas.md        # Documentation of queries in this repo
```

## SQL Query Conventions

### File Naming

- Use lowercase with underscores: `token_transfer_analysis.sql`
- Include the main table or protocol: `uniswap_v3_swaps.sql`
- Be descriptive but concise: `daily_active_addresses.sql`

### Query File Structure

Each SQL file should include a header comment block:

```sql
-- ============================================================
-- Query: [Descriptive Name]
-- Description: [What the query does]
-- Author: [GitHub username or name]
-- Created: [YYYY-MM-DD]
-- Updated: [YYYY-MM-DD]
-- Dune Link: [Optional - link to live Dune query]
-- ============================================================
-- Parameters:
--   {{blockchain}} - Target blockchain (default: ethereum)
--   {{start_date}} - Analysis start date
--   {{end_date}} - Analysis end date
-- ============================================================

-- Query begins here
SELECT ...
```

### SQL Style Guide

1. **Keywords**: Use UPPERCASE for SQL keywords (`SELECT`, `FROM`, `WHERE`, `JOIN`)
2. **Identifiers**: Use lowercase for table and column names
3. **Indentation**: Use 2 or 4 spaces consistently (not tabs)
4. **Line breaks**:
   - New line for each major clause (`SELECT`, `FROM`, `WHERE`, etc.)
   - New line for each column in SELECT (for readability)
5. **Aliases**: Use meaningful aliases (`t` for transactions, `b` for blocks)
6. **Comments**: Use `--` for single-line comments

### Example Query Format

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

## Dune Analytics Specifics

### Common Tables

**Ethereum:**
- `ethereum.transactions` - All transactions
- `ethereum.traces` - Internal transactions
- `ethereum.logs` - Event logs
- `tokens.erc20` - ERC20 token metadata
- `prices.usd` - Token prices

**Decoded Tables (Protocol-Specific):**
- `uniswap_v3_ethereum.Pair_evt_Swap`
- `aave_v3_ethereum.Pool_evt_Supply`
- `opensea_v2_ethereum.SeaportAdvanced_evt_OrderFulfilled`

### Dune Parameters

Use double curly braces for parameters:
- `{{blockchain}}` - Chain selector
- `{{start_date}}` - Date parameter
- `{{wallet_address}}` - Address parameter
- `{{token_symbol}}` - Token selector

### Spellbook Integration

[Dune Spellbook](https://github.com/duneanalytics/spellbook) provides curated, tested data models:
- `dex.trades` - Unified DEX trades across protocols
- `nft.trades` - Unified NFT trades
- `transfers.ethereum_erc20` - Token transfers

Prefer Spellbook tables over raw tables when available.

## Development Workflow

### Adding New Queries

1. Create the query file in the appropriate directory
2. Include the standard header comment block
3. Test the query on Dune before committing
4. Add any necessary documentation

### Testing Queries

- Always test queries on Dune Analytics before committing
- Verify results against known data points when possible
- Check query execution time and optimize if needed
- Test with different parameter values

### Optimization Tips

1. **Filter early**: Apply WHERE clauses as early as possible
2. **Limit date ranges**: Always include date filters for large tables
3. **Avoid SELECT ***: Only select columns you need
4. **Use appropriate aggregations**: Pre-aggregate when possible
5. **Index usage**: Filter on indexed columns (block_time, block_number)

## AI Assistant Guidelines

### When Writing Queries

1. Always include the header comment block
2. Use Dune SQL syntax (Trino/Presto SQL dialect)
3. Include date range parameters for time-series queries
4. Add comments for complex logic
5. Optimize for readability and performance

### When Modifying Existing Queries

1. Update the "Updated" date in the header
2. Preserve the original author information
3. Document what was changed and why
4. Test the modified query before committing

### When Reviewing Queries

Check for:
- SQL injection vulnerabilities (though Dune sanitizes parameters)
- Missing date filters on large tables
- Inefficient JOINs or subqueries
- Correct blockchain/table references
- Parameter usage for configurable values

### Common Mistakes to Avoid

1. Forgetting to divide token amounts by decimals (e.g., `/ 1e18` for ETH)
2. Using wrong table names for different chains
3. Missing NULL handling in aggregations
4. Not accounting for blockchain reorgs in recent data
5. Hardcoding values that should be parameters

## Git Workflow

### Commit Messages

Use clear, descriptive commit messages:
- `Add: uniswap v3 liquidity analysis query`
- `Fix: correct decimal handling in token transfers`
- `Update: optimize daily volume query performance`
- `Docs: add documentation for NFT queries`

### Branch Naming

- `feature/query-name` - New queries
- `fix/issue-description` - Bug fixes
- `docs/topic` - Documentation updates

## Resources

**Dune Documentation:**
- [Dune Analytics Documentation](https://docs.dune.com/)
- [Dune API Documentation](https://docs.dune.com/api-reference/overview)
- [Dune Documentation Index (for LLMs)](https://docs.dune.com/llms.txt)

**Data References:**
- [Blockchain Data Tables Reference](https://docs.dune.com/data-tables/)
- [Dune Spellbook](https://github.com/duneanalytics/spellbook)

**Local Documentation (Reliable Offline References):**
- [`docs/dune_database_schemas.md`](./docs/dune_database_schemas.md) - Dune table schemas
- [`docs/queries_schemas.md`](./docs/queries_schemas.md) - Repository query documentation

**SQL Reference:**
- [Trino SQL Documentation](https://trino.io/docs/current/)

## Quick Reference

### Useful SQL Patterns

**Convert wei to ETH:**
```sql
value / 1e18 AS eth_amount
```

**Get USD value:**
```sql
SELECT
    t.value / 1e18 * p.price AS usd_value
FROM ethereum.transactions t
LEFT JOIN prices.usd p ON p.symbol = 'ETH'
    AND p.minute = date_trunc('minute', t.block_time)
```

**Address formatting:**
```sql
-- Lowercase addresses
LOWER(address) AS normalized_address

-- Checksum format (display)
'0x' || encode(address, 'hex')
```

**Time aggregations:**
```sql
date_trunc('day', block_time) AS day
date_trunc('week', block_time) AS week
date_trunc('month', block_time) AS month
```
