# Documentation

This folder contains reference documentation for the DuneQueries repository.

## Contents

| File | Description |
|------|-------------|
| [dune_database_schemas.md](./dune_database_schemas.md) | Comprehensive reference of Dune Analytics table schemas (Ethereum, Bitcoin, Spellbook, etc.) |
| [queries_schemas.md](./queries_schemas.md) | Documentation of all queries in this repository with their input/output schemas |
| [query_constants.md](./query_constants.md) | Ledger of hardcoded thresholds, intervals, address allowlists, and other fixed constants |

---

## Contributing Queries

### Workflow

1. **Create the SQL file** in the appropriate blockchain directory under `/queries/`
2. **Include the standard header** (see [CLAUDE.md](../CLAUDE.md) for format)
3. **Validate against schemas** in [dune_database_schemas.md](./dune_database_schemas.md)
4. **Test on Dune Analytics** before committing
5. **Document in queries_schemas.md** following the template below

### Directory Structure

```
queries/
├── bitcoin/           # Bitcoin network queries
├── ethereum/          # Ethereum mainnet queries
├── base/              # Base chain queries
├── polygon/           # Polygon queries
├── arbitrum/          # Arbitrum queries
├── optimism/          # Optimism queries
└── cross-chain/       # Multi-chain queries
```

### Query Registries

Query metadata is stored in chain-specific registry files:

- `queries/registry.bitcoin.json`
- `queries/registry.ethereum.json`
- `queries/registry.base.json`

### Time-Scoped Note: Base Lending Loops

As of **2026-02-25 16:31:27 UTC** (**2026-02-25 17:31:27 CET +0100**), Base lending loop queries in this repository may return empty results for recent windows. This is an observed market-state outcome (very low recent Aave stablecoin activity and near-zero Aave/Morpho overlap), not necessarily a query defect.

This interpretation is time-dependent and should be revalidated on fresh data before reuse in future analysis or dashboards.

### Query File Naming

- Use lowercase with underscores: `token_transfer_analysis.sql`
- Include the main table or protocol: `uniswap_v3_swaps.sql`
- Be descriptive but concise: `daily_active_addresses.sql`

---

## Query Documentation Template

When adding a new query, document it in [queries_schemas.md](./queries_schemas.md) using this template:

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

## Schema Validation

Before committing any query, validate all table and column references against [dune_database_schemas.md](./dune_database_schemas.md).

**Validation Checklist:**

- [ ] All referenced tables exist in `dune_database_schemas.md`
- [ ] All column names match documented schemas
- [ ] Data types are used correctly
- [ ] Query executes without errors on Dune
- [ ] Results are reasonable (sanity check)

**Common Issues to Avoid:**

| Issue | Example | Correct |
|-------|---------|---------|
| Wrong column name | `timestamp` | `block_time` |
| Wrong schema prefix | `btc.inputs` | `bitcoin.inputs` |
| Missing decimals | `value` (raw) | `value / 1e18` |
| Non-existent table | `bitcoin.transactions.inputs` | `bitcoin.inputs` |

---

## See Also

- [CLAUDE.md](../CLAUDE.md) - Development guidelines and SQL conventions
- [Dune Documentation](https://docs.dune.com/)
- [Dune Spellbook](https://github.com/duneanalytics/spellbook)
