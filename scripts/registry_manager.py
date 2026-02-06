"""
Query registry management CLI.

Provides commands to list, update, and validate the query registry.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any


# Base path for the repository
REPO_ROOT = Path(__file__).parent.parent
REGISTRY_PATH = REPO_ROOT / "queries" / "registry.json"


def load_registry() -> dict[str, Any]:
    """Load the query registry from JSON file."""
    if not REGISTRY_PATH.exists():
        raise FileNotFoundError(f"Registry not found: {REGISTRY_PATH}")

    with open(REGISTRY_PATH) as f:
        return json.load(f)


def save_registry(registry: dict[str, Any]) -> None:
    """Save the query registry to JSON file."""
    with open(REGISTRY_PATH, "w") as f:
        json.dump(registry, f, indent=2)
        f.write("\n")  # Trailing newline


def get_query(name: str) -> dict[str, Any] | None:
    """Get a query entry by name."""
    registry = load_registry()
    for query in registry["queries"]:
        if query["name"] == name:
            return query
    return None


def set_query_id(name: str, dune_id: int) -> bool:
    """
    Set the Dune query ID for a query.

    Args:
        name: Query name in registry.
        dune_id: Dune query ID to set.

    Returns:
        True if successful, False if query not found.
    """
    registry = load_registry()

    for query in registry["queries"]:
        if query["name"] == name:
            query["dune_query_id"] = dune_id
            save_registry(registry)
            return True

    return False


def list_queries(
    architecture: str | None = None,
    query_type: str | None = None,
    with_smoke_test: bool | None = None,
) -> list[dict[str, Any]]:
    """
    List queries from registry with optional filters.

    Args:
        architecture: Filter by architecture ('v2', 'legacy').
        query_type: Filter by type ('base', 'nested', 'standalone').
        with_smoke_test: If True, only queries with smoke tests. If False, only without.

    Returns:
        List of matching query entries.
    """
    registry = load_registry()
    queries = registry["queries"]

    # Apply filters
    if architecture:
        queries = [q for q in queries if q.get("architecture") == architecture]

    if query_type:
        queries = [q for q in queries if q.get("type") == query_type]

    if with_smoke_test is not None:
        if with_smoke_test:
            queries = [q for q in queries if q.get("smoke_test")]
        else:
            queries = [q for q in queries if not q.get("smoke_test")]

    return queries


def validate_registry() -> list[str]:
    """
    Validate the registry for consistency.

    Checks:
    - All query files exist
    - All smoke test files exist
    - Dependencies reference valid queries
    - No duplicate query names

    Returns:
        List of error messages (empty if valid).
    """
    errors = []
    registry = load_registry()

    # Build set of known query names
    names = set()
    for query in registry["queries"]:
        name = query["name"]
        if name in names:
            errors.append(f"Duplicate query name: {name}")
        names.add(name)

    # Validate each query
    for query in registry["queries"]:
        name = query["name"]

        # Check query file exists
        query_file = REPO_ROOT / query["file"]
        if not query_file.exists():
            errors.append(f"[{name}] Query file not found: {query['file']}")

        # Check smoke test file exists (if defined)
        smoke_test = query.get("smoke_test")
        if smoke_test:
            smoke_path = REPO_ROOT / smoke_test
            if not smoke_path.exists():
                errors.append(f"[{name}] Smoke test not found: {smoke_test}")

        # Check dependencies exist
        for dep in query.get("dependencies", []):
            if dep not in names:
                errors.append(f"[{name}] Unknown dependency: {dep}")

        # Check nested queries have base query ID if they need it
        if query.get("type") == "nested" and query.get("dependencies"):
            # Check if base query has an ID set
            for dep in query["dependencies"]:
                dep_query = get_query(dep)
                if dep_query and not dep_query.get("dune_query_id"):
                    errors.append(
                        f"[{name}] Dependency '{dep}' has no Dune query ID set"
                    )

    return errors


def print_query_table(queries: list[dict[str, Any]]) -> None:
    """Print queries in a formatted table."""
    if not queries:
        print("No queries found.")
        return

    # Column headers and widths
    headers = ["Name", "Type", "Arch", "Dune ID", "Smoke Test"]
    widths = [35, 10, 8, 12, 8]

    # Print header
    header_row = " | ".join(h.ljust(w) for h, w in zip(headers, widths))
    print(header_row)
    print("-" * len(header_row))

    # Print rows
    for q in queries:
        dune_id = str(q.get("dune_query_id") or "-")
        has_smoke = "Yes" if q.get("smoke_test") else "No"
        row = [
            q["name"][:widths[0]],
            (q.get("type") or "-")[:widths[1]],
            (q.get("architecture") or "-")[:widths[2]],
            dune_id[:widths[3]],
            has_smoke,
        ]
        print(" | ".join(val.ljust(w) for val, w in zip(row, widths)))


def cmd_list(args: argparse.Namespace) -> int:
    """Handle 'list' command."""
    queries = list_queries(
        architecture=args.architecture,
        query_type=args.type,
        with_smoke_test=args.with_smoke_test,
    )

    print(f"\nQuery Registry ({len(queries)} queries)")
    print("=" * 80)
    print_query_table(queries)
    print()

    return 0


def cmd_show(args: argparse.Namespace) -> int:
    """Handle 'show' command."""
    query = get_query(args.name)

    if not query:
        print(f"Error: Query '{args.name}' not found in registry")
        return 1

    print(f"\nQuery: {query['name']}")
    print("-" * 40)
    print(json.dumps(query, indent=2))

    return 0


def cmd_set_id(args: argparse.Namespace) -> int:
    """Handle 'set-id' command."""
    if set_query_id(args.name, args.dune_id):
        print(f"Updated '{args.name}' with Dune query ID: {args.dune_id}")
        return 0
    else:
        print(f"Error: Query '{args.name}' not found in registry")
        return 1


def cmd_validate(args: argparse.Namespace) -> int:
    """Handle 'validate' command."""
    print("\nValidating registry...")
    print("-" * 40)

    errors = validate_registry()

    if not errors:
        print("[+] Registry is valid!")
        return 0
    else:
        print(f"[X] Found {len(errors)} error(s):\n")
        for error in errors:
            print(f"  - {error}")
        return 1


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Manage the Dune query registry",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m scripts.registry_manager list
  python -m scripts.registry_manager list --architecture v2
  python -m scripts.registry_manager show bitcoin_tx_features_daily
  python -m scripts.registry_manager set-id bitcoin_tx_features_daily 12345678
  python -m scripts.registry_manager validate
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # List command
    list_parser = subparsers.add_parser("list", help="List queries in registry")
    list_parser.add_argument(
        "--architecture",
        choices=["v2", "legacy"],
        help="Filter by architecture",
    )
    list_parser.add_argument(
        "--type",
        choices=["base", "nested", "standalone"],
        help="Filter by query type",
    )
    list_parser.add_argument(
        "--with-smoke-test",
        action="store_true",
        dest="with_smoke_test",
        default=None,
        help="Only show queries with smoke tests",
    )

    # Show command
    show_parser = subparsers.add_parser("show", help="Show details for a query")
    show_parser.add_argument("name", help="Query name")

    # Set-id command
    setid_parser = subparsers.add_parser("set-id", help="Set Dune query ID")
    setid_parser.add_argument("name", help="Query name")
    setid_parser.add_argument("dune_id", type=int, help="Dune query ID")

    # Validate command
    subparsers.add_parser("validate", help="Validate registry consistency")

    args = parser.parse_args()

    if args.command == "list":
        return cmd_list(args)
    elif args.command == "show":
        return cmd_show(args)
    elif args.command == "set-id":
        return cmd_set_id(args)
    elif args.command == "validate":
        return cmd_validate(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
