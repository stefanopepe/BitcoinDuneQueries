"""
Smoke test runner for Dune queries.

Executes smoke tests via Dune API and validates results.
"""

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

# Base path for the repository
REPO_ROOT = Path(__file__).parent.parent
OUTPUTS_DIR = REPO_ROOT / "outputs"
REGISTRY_SOURCES = [
    ("bitcoin", REPO_ROOT / "queries" / "registry.bitcoin.json"),
    ("ethereum", REPO_ROOT / "queries" / "registry.ethereum.json"),
    ("base", REPO_ROOT / "queries" / "registry.base.json"),
    ("intent", REPO_ROOT / "queries" / "registry.intent.json"),
]


@dataclass
class SmokeTestResult:
    """Result of a smoke test execution."""

    name: str
    success: bool
    execution_result: Any | None
    validations: list[Any]
    error: str | None = None

    @property
    def summary(self) -> str:
        """Get a summary string for the test result."""
        if self.error:
            return f"ERROR: {self.error}"

        passed = sum(1 for v in self.validations if v.passed)
        total = len(self.validations)

        if self.success:
            return f"PASSED ({passed}/{total} validations)"
        else:
            failed = [v for v in self.validations if not v.passed]
            failed_names = [v.check_name for v in failed]
            return f"FAILED ({passed}/{total} validations, failed: {failed_names})"


def load_registry() -> dict[str, Any]:
    """Load and merge all chain registries."""
    merged_queries: list[dict[str, Any]] = []
    for chain, path in REGISTRY_SOURCES:
        if not path.exists():
            raise FileNotFoundError(f"Registry not found: {path}")
        with open(path) as f:
            registry = json.load(f)
        for query in registry.get("queries", []):
            q = dict(query)
            q["_chain"] = chain
            q.setdefault("active", True)
            q.setdefault("refresh_window_days", 90)
            q.setdefault("execution_tier", "core")
            merged_queries.append(q)
    return {
        "version": "1.0",
        "description": "Merged query registry",
        "queries": merged_queries,
    }


def get_query_info(name: str) -> dict[str, Any] | None:
    """Get query info from registry by name."""
    registry = load_registry()
    for query in registry["queries"]:
        if query["name"] == name:
            return query
    return None


def format_window_bounds(window_days: int) -> tuple[str, str]:
    """Return UTC timestamp bounds for a rolling window."""
    end_dt = datetime.now(UTC)
    start_dt = end_dt - timedelta(days=window_days)
    return (
        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    )


def apply_window_params(sql: str, window_days: int) -> str:
    """Replace lightweight window placeholders used by local SQL utilities."""
    start_ts, end_ts = format_window_bounds(window_days)
    return (
        sql.replace("{start_ts}", start_ts)
        .replace("{end_ts}", end_ts)
        .replace("{window_days}", str(window_days))
    )


def assert_no_unresolved_placeholders(sql: str) -> None:
    """Fail fast if nested query placeholders remain unresolved."""
    import re

    unresolved = sorted(set(re.findall(r"query_<([A-Z0-9_]+)>", sql)))
    if unresolved:
        raise ValueError(
            "Unresolved query placeholders after substitution: "
            + ", ".join(unresolved)
        )


def query_matches_filters(
    query: dict[str, Any],
    chain: str,
    active_only: bool,
    tier: str,
    architecture: str | None,
) -> bool:
    """Check if a query is in the current execution scope."""
    if architecture and query.get("architecture") != architecture:
        return False
    if chain != "all" and query.get("_chain") != chain:
        return False
    if active_only and not bool(query.get("active", True)):
        return False
    if tier != "all" and query.get("execution_tier", "core") != tier:
        return False
    return bool(query.get("smoke_test"))


def load_smoke_test_sql(test_path: str) -> str:
    """
    Load smoke test SQL from file.

    Args:
        test_path: Relative path to smoke test file from repo root.

    Returns:
        SQL string contents of the smoke test file.
    """
    full_path = REPO_ROOT / test_path
    if not full_path.exists():
        raise FileNotFoundError(f"Smoke test not found: {full_path}")

    with open(full_path) as f:
        return f.read()


def substitute_query_ids(sql: str, registry: dict[str, Any]) -> str:
    """
    Substitute query_<SOME_QUERY_NAME_ID> placeholders with actual Dune IDs.

    Args:
        sql: SQL string with potential placeholders.
        registry: Query registry dict.

    Returns:
        SQL with placeholders replaced.

    Raises:
        ValueError: If a dependency's query ID is not set in the registry.
    """
    # Build a map of query names to their Dune IDs
    id_map = {}
    for query in registry["queries"]:
        if query["dune_query_id"]:
            id_map[query["name"]] = query["dune_query_id"]

    import re

    def to_query_name(token: str) -> str:
        # BASE_LENDING_FLOW_STITCHING_ID -> base_lending_flow_stitching
        name = token
        if name.endswith("_ID"):
            name = name[:-3]
        return name.lower()

    def replace_placeholder(match: re.Match) -> str:
        token = match.group(1)
        qname = to_query_name(token)
        dune_id = id_map.get(qname)
        if dune_id:
            return f"query_{dune_id}"
        return match.group(0)

    return re.sub(r"query_<([A-Z0-9_]+)>", replace_placeholder, sql)


def run_smoke_test(
    name: str,
    timeout_seconds: int = 300,
    window_days: int = 90,
) -> SmokeTestResult:
    """
    Run a smoke test for a query.

    Args:
        name: Query name from registry.
        timeout_seconds: Maximum time to wait for execution.

    Returns:
        SmokeTestResult with execution and validation results.
    """
    # Get query info from registry
    query_info = get_query_info(name)
    if not query_info:
        return SmokeTestResult(
            name=name,
            success=False,
            execution_result=None,
            validations=[],
            error=f"Query '{name}' not found in registry",
        )

    # Check if smoke test exists
    smoke_test_path = query_info.get("smoke_test")
    if not smoke_test_path:
        return SmokeTestResult(
            name=name,
            success=False,
            execution_result=None,
            validations=[],
            error=f"No smoke test defined for query '{name}'",
        )

    try:
        # Load smoke test SQL
        sql = load_smoke_test_sql(smoke_test_path)

        # Substitute query IDs and enforce placeholder resolution.
        registry = load_registry()
        sql = substitute_query_ids(sql, registry)
        assert_no_unresolved_placeholders(sql)
        sql = apply_window_params(sql, window_days)

        # Execute the smoke test
        from scripts.dune_client import execute_sql
        from scripts.validators import validate_execution_success, validate_non_empty

        print(f"  Executing smoke test for '{name}'...")
        result = execute_sql(sql, timeout_seconds=timeout_seconds)

        # Run validations
        validations = [
            validate_execution_success(result),
            validate_non_empty(result),
        ]

        # Check if all validations passed
        all_passed = all(v.passed for v in validations)

        return SmokeTestResult(
            name=name,
            success=all_passed,
            execution_result=result,
            validations=validations,
        )

    except FileNotFoundError as e:
        return SmokeTestResult(
            name=name,
            success=False,
            execution_result=None,
            validations=[],
            error=str(e),
        )
    except Exception as e:
        return SmokeTestResult(
            name=name,
            success=False,
            execution_result=None,
            validations=[],
            error=f"Unexpected error: {e}",
        )


def run_all_smoke_tests(
    chain: str = "base",
    active_only: bool = True,
    tier: str = "core",
    architecture: str | None = None,
    timeout_seconds: int = 300,
    window_days: int = 90,
) -> list[SmokeTestResult]:
    """
    Run all smoke tests in the registry.

    Args:
        architecture: Optional filter for query architecture ('v2', 'legacy').
        timeout_seconds: Maximum time to wait per execution.

    Returns:
        List of SmokeTestResult for each query with a smoke test.
    """
    registry = load_registry()
    results = []

    for query in registry["queries"]:
        if not query_matches_filters(query, chain, active_only, tier, architecture):
            continue
        result = run_smoke_test(
            query["name"],
            timeout_seconds=timeout_seconds,
            window_days=window_days,
        )
        results.append(result)

    return results


def list_available_tests(
    chain: str = "base",
    active_only: bool = True,
    tier: str = "core",
    architecture: str | None = None,
) -> list[dict[str, Any]]:
    """List all queries that have smoke tests defined."""
    registry = load_registry()
    tests = []

    for query in registry["queries"]:
        if not query_matches_filters(query, chain, active_only, tier, architecture):
            continue
        tests.append(
            {
                "name": query["name"],
                "smoke_test": query["smoke_test"],
                "architecture": query.get("architecture", "unknown"),
                "type": query.get("type", "unknown"),
                "chain": query.get("_chain", "unknown"),
                "active": bool(query.get("active", True)),
                "execution_tier": query.get("execution_tier", "core"),
            }
        )

    return tests


def _pick_first(d: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
    return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_execution_inventory(
    chain: str,
    active_only: bool,
    tier: str,
    architecture: str | None,
) -> dict[str, Any]:
    """Collect latest query execution metadata for current scope."""
    from scripts.dune_client import get_execution_status, get_query_details

    registry = load_registry()
    items: list[dict[str, Any]] = []
    for query in registry["queries"]:
        if not query_matches_filters(query, chain, active_only, tier, architecture):
            continue

        item = {
            "name": query["name"],
            "chain": query.get("_chain"),
            "active": bool(query.get("active", True)),
            "execution_tier": query.get("execution_tier", "core"),
            "refresh_window_days": int(query.get("refresh_window_days", 90)),
            "query_id": query.get("dune_query_id"),
            "last_state": None,
            "runtime_ms": None,
            "credits": None,
            "last_run_time": None,
            "latest_execution_id": None,
            "error": None,
        }

        query_id = query.get("dune_query_id")
        if not query_id:
            item["error"] = "missing_dune_query_id"
            items.append(item)
            continue

        try:
            details = get_query_details(int(query_id))
            latest_execution_id = _pick_first(
                details,
                ["latest_execution_id", "execution_id"],
            )
            latest_state = _pick_first(
                details,
                ["latest_execution_state", "state", "query_state"],
            )
            status: dict[str, Any] = {}
            if latest_execution_id:
                status = get_execution_status(str(latest_execution_id))

            item["latest_execution_id"] = latest_execution_id
            item["last_state"] = _pick_first(
                status if status else details,
                ["state", "query_state", "latest_execution_state"],
            ) or latest_state
            item["runtime_ms"] = _pick_first(
                status if status else details,
                [
                    "execution_time_millis",
                    "execution_time_ms",
                    "duration_ms",
                    "query_run_time_ms",
                ],
            )
            item["credits"] = _pick_first(
                status if status else details,
                [
                    "credits",
                    "credits_used",
                    "execution_credits",
                ],
            )
            item["last_run_time"] = _pick_first(
                status if status else details,
                [
                    "execution_started_at",
                    "submitted_at",
                    "completed_at",
                    "created_at",
                ],
            )
            if (
                item["last_state"] is None
                and item["runtime_ms"] is None
                and item["credits"] is None
                and item["last_run_time"] is None
            ):
                item["error"] = "metadata_unavailable"
        except Exception as e:
            item["error"] = str(e)

        items.append(item)

    items.sort(key=lambda x: str(x["name"]))
    return {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "chain_scope": chain,
        "active_only": active_only,
        "execution_tier": tier,
        "architecture": architecture or "all",
        "queries": items,
    }


def write_execution_inventory(inventory: dict[str, Any]) -> Path:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUTS_DIR / "base_mcp_execution_inventory.json"
    out_path.write_text(json.dumps(inventory, indent=2) + "\n")
    return out_path


def write_kpi_report(
    inventory: dict[str, Any],
    results: list[SmokeTestResult],
    window_days: int,
) -> Path:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUTS_DIR / "base_lending_intent_kpi_report.md"

    result_by_name = {r.name: r for r in results}
    rows = inventory.get("queries", [])
    core_count = sum(1 for row in rows if row.get("execution_tier") == "core")
    serving_count = sum(1 for row in rows if row.get("execution_tier") == "serving")
    active_count = sum(1 for row in rows if row.get("active"))
    completed_count = sum(1 for row in rows if row.get("last_state") == "QUERY_STATE_COMPLETED")
    credits_vals = [_to_float(row.get("credits")) for row in rows]
    credits_sum = sum(v for v in credits_vals if v is not None)
    ranked = sorted(
        rows,
        key=lambda row: _to_float(row.get("credits")) or -1.0,
        reverse=True,
    )
    top_rows = ranked[:5]

    lines = [
        "# Base Lending Intent KPI Report",
        "",
        f"- Generated at (UTC): `{inventory.get('generated_at')}`",
        f"- Scope: chain=`{inventory.get('chain_scope')}`, tier=`{inventory.get('execution_tier')}`, active_only=`{inventory.get('active_only')}`",
        f"- Window: rolling `{window_days}` days (full refresh)",
        "",
        "## Reliability",
        "",
        f"- Active queries in scope: **{active_count}**",
        f"- Core queries: **{core_count}**",
        f"- Serving queries: **{serving_count}**",
        f"- Latest completed state count: **{completed_count}/{len(rows)}**",
        "",
        "## Smoke Execution",
        "",
        f"- Executed tests: **{len(results)}**",
        f"- Passed: **{sum(1 for r in results if r.success)}**",
        f"- Failed: **{sum(1 for r in results if not r.success)}**",
        "",
        "## Efficiency Snapshot",
        "",
        f"- Sum of latest reported credits in scope: **{credits_sum:.6f}**",
        "",
        "| Query | Tier | Latest state | Credits | Runtime ms | Smoke status |",
        "|---|---|---|---:|---:|---|",
    ]

    for row in top_rows:
        name = str(row.get("name"))
        smoke = result_by_name.get(name)
        smoke_state = "PASS" if smoke and smoke.success else ("FAIL" if smoke else "n/a")
        credits = _to_float(row.get("credits"))
        runtime = _to_float(row.get("runtime_ms"))
        lines.append(
            f"| `{name}` | `{row.get('execution_tier')}` | `{row.get('last_state')}` | "
            f"{'' if credits is None else f'{credits:.6f}'} | "
            f"{'' if runtime is None else f'{runtime:.0f}'} | `{smoke_state}` |"
        )

    lines.extend(
        [
            "",
            "## Trend-Quality Gate (Manual Checklist)",
            "",
            "- Loop share trend looks directionally stable on rolling weekly view.",
            "- Borrow volume by intent bucket has no unexplained structural discontinuities.",
            "- Unique borrower trend by intent bucket remains interpretable after refactor.",
            "",
            "## Notes",
            "",
            "- Report is generated from latest query metadata and smoke-run outcomes.",
            "- Use this report as rollout evidence for Base-first gates before extending horizon.",
            "",
        ]
    )

    out_path.write_text("\n".join(lines))
    return out_path


def print_results(results: list[SmokeTestResult]) -> None:
    """Print formatted test results to stdout."""
    print("\n" + "=" * 60)
    print("SMOKE TEST RESULTS")
    print("=" * 60)

    passed = 0
    failed = 0

    for result in results:
        status = "PASS" if result.success else "FAIL"
        icon = "[+]" if result.success else "[X]"
        print(f"\n{icon} {result.name}: {status}")
        print(f"    {result.summary}")

        if result.execution_result and result.execution_result.row_count > 0:
            print(f"    Rows returned: {result.execution_result.row_count}")

        if not result.success:
            for v in result.validations:
                if not v.passed:
                    print(f"    - {v.check_name}: {v.message}")

        if result.success:
            passed += 1
        else:
            failed += 1

    print("\n" + "-" * 60)
    print(f"TOTAL: {passed} passed, {failed} failed, {len(results)} total")
    print("=" * 60)


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Run smoke tests for Dune queries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m scripts.smoke_runner --test bitcoin_tx_features_daily
  python -m scripts.smoke_runner --all --chain base --tier core
  python -m scripts.smoke_runner --all --chain base --tier serving
  python -m scripts.smoke_runner --list
        """,
    )

    parser.add_argument(
        "--test",
        "-t",
        help="Run smoke test for a specific query by name",
    )
    parser.add_argument(
        "--all",
        "-a",
        action="store_true",
        help="Run all available smoke tests",
    )
    parser.add_argument(
        "--architecture",
        choices=["v2", "legacy"],
        help="Filter tests by architecture (only with --all)",
    )
    parser.add_argument(
        "--chain",
        choices=["base", "ethereum", "bitcoin", "intent", "all"],
        default="base",
        help="Registry chain scope (default: base)",
    )
    parser.add_argument(
        "--active-only",
        type=lambda v: str(v).strip().lower() not in {"0", "false", "no", "off"},
        nargs="?",
        const=True,
        default=True,
        help="Only include active queries (default: true)",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=90,
        help="Rolling window for placeholder substitution (default: 90)",
    )
    parser.add_argument(
        "--tier",
        choices=["core", "serving", "all"],
        default="core",
        help="Execution tier scope (default: core)",
    )
    parser.add_argument(
        "--use-mcp-metrics",
        action="store_true",
        help="Collect latest execution metadata and write inventory/report artifacts",
    )
    parser.add_argument(
        "--inventory-only",
        action="store_true",
        help="Only write inventory/KPI artifacts for current scope (no smoke execution)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Timeout in seconds for each test (default: 300)",
    )
    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="List all available smoke tests",
    )

    args = parser.parse_args()

    # List mode
    if args.list:
        tests = list_available_tests(
            chain=args.chain,
            active_only=args.active_only,
            tier=args.tier,
            architecture=args.architecture,
        )
        print("\nAvailable smoke tests:")
        print("-" * 60)
        for test in tests:
            print(f"  {test['name']}")
            print(f"    File: {test['smoke_test']}")
            print(
                "    "
                f"Chain: {test['chain']}, "
                f"Architecture: {test['architecture']}, "
                f"Type: {test['type']}, "
                f"Tier: {test['execution_tier']}, "
                f"Active: {test['active']}"
            )
        print(f"\nTotal: {len(tests)} tests")
        return 0

    if args.inventory_only:
        inventory = build_execution_inventory(
            chain=args.chain,
            active_only=args.active_only,
            tier=args.tier,
            architecture=args.architecture,
        )
        inventory_path = write_execution_inventory(inventory)
        report_path = write_kpi_report(
            inventory=inventory,
            results=[],
            window_days=args.window_days,
        )
        print(f"Wrote inventory: {inventory_path}")
        print(f"Wrote KPI report: {report_path}")
        return 0

    # Run single test
    if args.test:
        print(f"\nRunning smoke test: {args.test}")
        result = run_smoke_test(
            args.test,
            timeout_seconds=args.timeout,
            window_days=args.window_days,
        )
        print_results([result])
        if args.use_mcp_metrics:
            inventory = build_execution_inventory(
                chain=args.chain,
                active_only=args.active_only,
                tier=args.tier,
                architecture=args.architecture,
            )
            inventory_path = write_execution_inventory(inventory)
            report_path = write_kpi_report(
                inventory=inventory,
                results=[result],
                window_days=args.window_days,
            )
            print(f"\nWrote inventory: {inventory_path}")
            print(f"Wrote KPI report: {report_path}")
        return 0 if result.success else 1

    # Run all tests
    if args.all:
        arch_str = f", architecture={args.architecture}" if args.architecture else ""
        print(
            "\nRunning all smoke tests "
            f"(chain={args.chain}, active_only={args.active_only}, tier={args.tier}{arch_str})..."
        )
        results = run_all_smoke_tests(
            chain=args.chain,
            active_only=args.active_only,
            tier=args.tier,
            architecture=args.architecture,
            timeout_seconds=args.timeout,
            window_days=args.window_days,
        )

        if not results:
            print("No smoke tests found matching criteria.")
            return 0

        print_results(results)
        if args.use_mcp_metrics:
            inventory = build_execution_inventory(
                chain=args.chain,
                active_only=args.active_only,
                tier=args.tier,
                architecture=args.architecture,
            )
            inventory_path = write_execution_inventory(inventory)
            report_path = write_kpi_report(
                inventory=inventory,
                results=results,
                window_days=args.window_days,
            )
            print(f"\nWrote inventory: {inventory_path}")
            print(f"Wrote KPI report: {report_path}")

        # Return non-zero if any test failed
        return 0 if all(r.success for r in results) else 1

    # No action specified
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
