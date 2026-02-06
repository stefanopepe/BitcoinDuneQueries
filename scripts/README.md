# Dune Query Scripts

Python utilities for managing Dune Analytics queries and running smoke tests programmatically.

## Setup

### Prerequisites

- Python 3.11+
- Dune Analytics API key ([get one here](https://dune.com/settings/api))

### Installation

```bash
# From repository root, create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install in editable mode
pip install -e .

# Or install dependencies directly
pip install dune-client python-dotenv pandas
```

### Configuration

1. Copy the environment template:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` and add your Dune API key:
   ```
   DUNE_API_KEY=your_api_key_here
   ```

## Scripts

### Smoke Test Runner (`smoke_runner.py`)

Execute smoke tests against Dune API to validate queries.

```bash
# List available smoke tests
python -m scripts.smoke_runner --list

# Run a specific smoke test
python -m scripts.smoke_runner --test bitcoin_tx_features_daily

# Run all smoke tests
python -m scripts.smoke_runner --all

# Run only V2 architecture tests
python -m scripts.smoke_runner --all --architecture v2

# Set custom timeout (default: 300 seconds)
python -m scripts.smoke_runner --test bitcoin_tx_features_daily --timeout 600
```

### Registry Manager (`registry_manager.py`)

Manage the query metadata registry.

```bash
# List all queries
python -m scripts.registry_manager list

# List only V2 queries
python -m scripts.registry_manager list --architecture v2

# List only nested queries
python -m scripts.registry_manager list --type nested

# Show details for a specific query
python -m scripts.registry_manager show bitcoin_tx_features_daily

# Set the Dune query ID for a query
python -m scripts.registry_manager set-id bitcoin_tx_features_daily 12345678

# Validate registry consistency
python -m scripts.registry_manager validate
```

## Query Registry

The query registry (`queries/registry.json`) maintains metadata about all queries:

```json
{
  "name": "bitcoin_tx_features_daily",
  "file": "queries/bitcoin/bitcoin_tx_features_daily.sql",
  "dune_query_id": null,
  "type": "base",
  "architecture": "v2",
  "smoke_test": "tests/bitcoin_tx_features_daily_smoke.sql",
  "dependencies": [],
  "description": "..."
}
```

### Fields

| Field | Description |
|-------|-------------|
| `name` | Unique identifier for the query |
| `file` | Path to SQL file (relative to repo root) |
| `dune_query_id` | Dune query ID (null if not yet created on Dune) |
| `type` | Query type: `base`, `nested`, or `standalone` |
| `architecture` | Architecture version: `v2` or `legacy` |
| `smoke_test` | Path to smoke test file (null if none) |
| `dependencies` | List of query names this query depends on |
| `description` | Human-readable description |

## Programmatic Usage

### Running Smoke Tests from Python

```python
from scripts.smoke_runner import run_smoke_test, run_all_smoke_tests

# Run a single test
result = run_smoke_test("bitcoin_tx_features_daily")
print(f"Success: {result.success}")
print(f"Rows: {result.execution_result.row_count}")

# Run all tests
results = run_all_smoke_tests(architecture="v2")
for r in results:
    print(f"{r.name}: {r.summary}")
```

### Executing Raw SQL

```python
from scripts.dune_client import execute_sql

sql = """
SELECT date_trunc('day', block_time) as day, COUNT(*) as tx_count
FROM bitcoin.transactions
WHERE block_time >= NOW() - INTERVAL '7' DAY
GROUP BY 1
ORDER BY 1
"""

result = execute_sql(sql)
if result.success:
    print(f"Returned {result.row_count} rows")
    for row in result.rows:
        print(row)
else:
    print(f"Error: {result.error}")
```

### Executing Saved Queries

```python
from scripts.dune_client import execute_query, get_latest_result

# Execute a saved query by ID
result = execute_query(query_id=12345678)

# Get cached results (avoids re-execution if recent)
result = get_latest_result(query_id=12345678, max_age_hours=8)
```

## Validation

The smoke runner performs these validations:

1. **Execution Success** - Query completed without errors
2. **Non-Empty Results** - Query returned at least one row

Additional validations can be configured:

```python
from scripts.validators import run_all_validations

validations = run_all_validations(
    result,
    expected_columns=["day", "score_band", "tx_count"],
    min_rows=5,
    value_ranges={"human_factor_score": (0, 100)},
    non_null_columns=["day", "tx_count"],
)

for v in validations:
    print(f"{v.check_name}: {'PASS' if v.passed else 'FAIL'} - {v.message}")
```

## Troubleshooting

### API Key Issues

```
ValueError: DUNE_API_KEY environment variable is not set
```

Ensure your `.env` file exists and contains a valid API key.

### Query Not Found

```
Error: Query 'xyz' not found in registry
```

Check the query name in `queries/registry.json`. Use `registry_manager list` to see available queries.

### Nested Query ID Not Set

```
[query_name] Dependency 'bitcoin_tx_features_daily' has no Dune query ID set
```

Nested queries reference a base query by ID. Set the base query ID first:

```bash
python -m scripts.registry_manager set-id bitcoin_tx_features_daily 12345678
```

### Timeout Errors

Increase the timeout for long-running queries:

```bash
python -m scripts.smoke_runner --test query_name --timeout 600
```
