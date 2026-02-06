"""
Smoke test runner for Dune queries.

Executes smoke tests via Dune API and validates results.
"""

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.dune_client import ExecutionResult, execute_sql
from scripts.validators import (
    ValidationResult,
    run_all_validations,
    validate_execution_success,
    validate_non_empty,
)


# Base path for the repository
REPO_ROOT = Path(__file__).parent.parent


@dataclass
class SmokeTestResult:
    """Result of a smoke test execution."""

    name: str
    success: bool
    execution_result: ExecutionResult | None
    validations: list[ValidationResult]
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
    """Load the query registry from JSON file."""
    registry_path = REPO_ROOT / "queries" / "registry.json"
    if not registry_path.exists():
        raise FileNotFoundError(f"Registry not found: {registry_path}")

    with open(registry_path) as f:
        return json.load(f)


def get_query_info(name: str) -> dict[str, Any] | None:
    """Get query info from registry by name."""
    registry = load_registry()
    for query in registry["queries"]:
        if query["name"] == name:
            return query
    return None


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
    Substitute query_<BASE_QUERY_ID> placeholders with actual Dune query IDs.

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

    # Check for placeholder pattern and substitute
    # Pattern: query_<BASE_QUERY_ID> where BASE_QUERY_ID could be a name or literal
    import re

    def replace_placeholder(match: re.Match) -> str:
        placeholder = match.group(0)
        # Try to find the base query (bitcoin_tx_features_daily)
        base_query_name = "bitcoin_tx_features_daily"
        if base_query_name in id_map:
            return f"query_{id_map[base_query_name]}"
        # If we can't substitute, return original (will fail at Dune execution)
        return placeholder

    # Replace query_<BASE_QUERY_ID> pattern
    result = re.sub(r"query_<BASE_QUERY_ID>", replace_placeholder, sql)

    return result


def run_smoke_test(
    name: str,
    timeout_seconds: int = 300,
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

        # Substitute query IDs if needed
        registry = load_registry()
        sql = substitute_query_ids(sql, registry)

        # Execute the smoke test
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
    architecture: str | None = None,
    timeout_seconds: int = 300,
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
        # Filter by architecture if specified
        if architecture and query.get("architecture") != architecture:
            continue

        # Skip queries without smoke tests
        if not query.get("smoke_test"):
            continue

        result = run_smoke_test(query["name"], timeout_seconds)
        results.append(result)

    return results


def list_available_tests() -> list[dict[str, Any]]:
    """List all queries that have smoke tests defined."""
    registry = load_registry()
    tests = []

    for query in registry["queries"]:
        if query.get("smoke_test"):
            tests.append(
                {
                    "name": query["name"],
                    "smoke_test": query["smoke_test"],
                    "architecture": query.get("architecture", "unknown"),
                    "type": query.get("type", "unknown"),
                }
            )

    return tests


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
  python -m scripts.smoke_runner --all
  python -m scripts.smoke_runner --all --architecture v2
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
        tests = list_available_tests()
        print("\nAvailable smoke tests:")
        print("-" * 60)
        for test in tests:
            print(f"  {test['name']}")
            print(f"    File: {test['smoke_test']}")
            print(f"    Architecture: {test['architecture']}, Type: {test['type']}")
        print(f"\nTotal: {len(tests)} tests")
        return 0

    # Run single test
    if args.test:
        print(f"\nRunning smoke test: {args.test}")
        result = run_smoke_test(args.test, args.timeout)
        print_results([result])
        return 0 if result.success else 1

    # Run all tests
    if args.all:
        arch_str = f" (architecture={args.architecture})" if args.architecture else ""
        print(f"\nRunning all smoke tests{arch_str}...")
        results = run_all_smoke_tests(args.architecture, args.timeout)

        if not results:
            print("No smoke tests found matching criteria.")
            return 0

        print_results(results)

        # Return non-zero if any test failed
        return 0 if all(r.success for r in results) else 1

    # No action specified
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
