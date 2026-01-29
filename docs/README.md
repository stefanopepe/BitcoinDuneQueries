# Documentation

This folder contains reference documentation for the DuneQueries repository.

## Contents

| File | Description |
|------|-------------|
| [dune_database_schemas.md](./dune_database_schemas.md) | Comprehensive reference of Dune Analytics table schemas (Ethereum, Bitcoin, Spellbook, etc.) |
| [queries_schemas.md](./queries_schemas.md) | Documentation of all queries in this repository with their input/output schemas |

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
├── polygon/           # Polygon queries
├── arbitrum/          # Arbitrum queries
├── optimism/          # Optimism queries
└── cross-chain/       # Multi-chain queries
```

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
