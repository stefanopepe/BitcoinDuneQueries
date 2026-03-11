# DuneQueries

A curated collection of SQL queries for [Dune Analytics](https://dune.com/), the blockchain analytics platform for querying on-chain data across Ethereum, Polygon, Arbitrum, Optimism, Solana, and more.

## Alpen SIGNAL Runner (ETH + Base)

This repo now includes a runnable BTC-collateral borrowing intent pipeline:

```bash
python3 -m src.run
```

Expected environment variables:

- `DUNE_API_KEY` (required)
- `LOOKBACK_DAYS` (optional, default `90`; use `7`/`14` for sample runs)

Outputs:

- `outputs/signal_report.md`
- `outputs/signal_metrics.csv`
- `outputs/borrow_flows_sample.csv`

## Overview

This repository provides:

- **Version-controlled queries** for blockchain data analysis
- **Reusable templates** and query patterns
- **Documented techniques** for on-chain analytics
- **Production-ready queries** tested against live data

## Dune MCP (Preferred)

Use Dune MCP as the primary context and tool surface for exploration and planning.

- Endpoint: `https://api.dune.com/mcp/v1`
- Canonical API key env var: `DUNE_API_KEY`
- Standard MCP server alias: `dune_prod`

Codex setup:

```bash
codex mcp add dune_prod --url "https://api.dune.com/mcp/v1?api_key=$DUNE_API_KEY"
```

Then set Codex MCP timeout (recommended by Dune guide to avoid `Transport closed` on long calls):

```toml
[mcp_servers.dune_prod]
url = "https://api.dune.com/mcp/v1?api_key=<YOUR_DUNE_API_KEY>"
tool_timeout_sec = 300
```

Codex manual setup (fallback if `codex mcp add` cannot write `~/.codex/config.toml`):

1. Ensure `~/.codex/config.toml` exists.
2. Add this block:

```toml
[mcp_servers.dune_prod]
url = "https://api.dune.com/mcp/v1?api_key=<YOUR_DUNE_API_KEY>"
tool_timeout_sec = 300
```

3. Verify:

```bash
codex mcp list
codex mcp get dune_prod
```

Expected: `dune_prod` is `enabled` with URL `https://api.dune.com/mcp/v1?...`.

Claude Code setup:

```bash
claude mcp add --scope user --transport http dune_prod https://api.dune.com/mcp/v1 --header "x-dune-api-key: $DUNE_API_KEY"
```

Use direct REST API calls only when MCP is unavailable or insufficient for the task.

Troubleshooting:

- `failed to persist config.toml ... Operation not permitted`:
  - Codex cannot write `~/.codex/config.toml` in your environment.
  - Apply the manual `config.toml` block above, or run Codex in a terminal/session with permission to write `~/.codex`.
- `Transport closed` during MCP calls:
  - Set `tool_timeout_sec = 300` in `[mcp_servers.dune_prod]` (official Dune MCP guidance for Codex).
- Keep API keys out of git and shell history when possible; prefer env-var expansion over literal keys.

## Development Pipeline

We follow a structured 4-step pipeline for developing and validating Dune queries:

```
Schema Exploration (MCP-first) → Query Development → Validation → Cost Estimation
```

### 1. Schema Exploration

Use Dune MCP first to explore tables/schemas and only fall back to direct API if needed.

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

Run smoke tests to catch errors. Tests can be run programmatically via the Dune API:

```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e .

# Configure API key
cp .env.example .env
# Edit .env with your DUNE_API_KEY

# Run smoke tests
python -m scripts.smoke_runner --list          # List available tests
python -m scripts.smoke_runner --test <name>   # Run specific test
python -m scripts.smoke_runner --all           # Run all tests
```

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
├── tests/                 # Smoke test SQL files
├── scripts/               # Python utilities for Dune API
│   ├── dune_client.py     # Dune API wrapper
│   ├── smoke_runner.py    # Smoke test execution
│   ├── validators.py      # Result validation
│   └── registry_manager.py # Query registry CLI
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
