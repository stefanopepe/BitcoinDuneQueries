"""
Result validation functions for smoke tests.

Provides validation checks to verify query results meet expectations.
"""

from dataclasses import dataclass
from typing import Any

from scripts.dune_client import ExecutionResult


@dataclass
class ValidationResult:
    """Result of a validation check."""

    passed: bool
    check_name: str
    message: str
    details: dict[str, Any] | None = None


def validate_execution_success(result: ExecutionResult) -> ValidationResult:
    """
    Check that the query execution completed successfully.

    Args:
        result: ExecutionResult from query execution.

    Returns:
        ValidationResult indicating pass/fail.
    """
    if result.success:
        return ValidationResult(
            passed=True,
            check_name="execution_success",
            message="Query executed successfully",
            details={"state": result.state, "execution_id": result.execution_id},
        )
    else:
        return ValidationResult(
            passed=False,
            check_name="execution_success",
            message=f"Query execution failed: {result.error}",
            details={"state": result.state, "error": result.error},
        )


def validate_non_empty(result: ExecutionResult) -> ValidationResult:
    """
    Check that the query returned at least one row.

    Args:
        result: ExecutionResult from query execution.

    Returns:
        ValidationResult indicating pass/fail.
    """
    if result.row_count > 0:
        return ValidationResult(
            passed=True,
            check_name="non_empty",
            message=f"Query returned {result.row_count} rows",
            details={"row_count": result.row_count},
        )
    else:
        return ValidationResult(
            passed=False,
            check_name="non_empty",
            message="Query returned no rows",
            details={"row_count": 0},
        )


def validate_columns(
    result: ExecutionResult,
    expected_columns: list[str],
    strict: bool = False,
) -> ValidationResult:
    """
    Check that the result contains expected columns.

    Args:
        result: ExecutionResult from query execution.
        expected_columns: List of column names that should be present.
        strict: If True, result must have exactly these columns (no extras).

    Returns:
        ValidationResult indicating pass/fail.
    """
    actual_columns = set(result.columns)
    expected_set = set(expected_columns)

    missing = expected_set - actual_columns
    extra = actual_columns - expected_set if strict else set()

    if not missing and not extra:
        return ValidationResult(
            passed=True,
            check_name="columns",
            message=f"All {len(expected_columns)} expected columns present",
            details={
                "expected": expected_columns,
                "actual": result.columns,
            },
        )
    else:
        issues = []
        if missing:
            issues.append(f"missing: {list(missing)}")
        if extra:
            issues.append(f"extra: {list(extra)}")

        return ValidationResult(
            passed=False,
            check_name="columns",
            message=f"Column mismatch: {'; '.join(issues)}",
            details={
                "expected": expected_columns,
                "actual": result.columns,
                "missing": list(missing),
                "extra": list(extra) if strict else None,
            },
        )


def validate_min_rows(
    result: ExecutionResult,
    min_rows: int,
) -> ValidationResult:
    """
    Check that the result has at least a minimum number of rows.

    Args:
        result: ExecutionResult from query execution.
        min_rows: Minimum number of rows expected.

    Returns:
        ValidationResult indicating pass/fail.
    """
    if result.row_count >= min_rows:
        return ValidationResult(
            passed=True,
            check_name="min_rows",
            message=f"Row count {result.row_count} >= minimum {min_rows}",
            details={"row_count": result.row_count, "min_rows": min_rows},
        )
    else:
        return ValidationResult(
            passed=False,
            check_name="min_rows",
            message=f"Row count {result.row_count} < minimum {min_rows}",
            details={"row_count": result.row_count, "min_rows": min_rows},
        )


def validate_value_in_range(
    result: ExecutionResult,
    column: str,
    min_value: float | None = None,
    max_value: float | None = None,
) -> ValidationResult:
    """
    Check that all values in a column fall within a specified range.

    Args:
        result: ExecutionResult from query execution.
        column: Column name to check.
        min_value: Minimum allowed value (inclusive), or None for no lower bound.
        max_value: Maximum allowed value (inclusive), or None for no upper bound.

    Returns:
        ValidationResult indicating pass/fail.
    """
    if column not in result.columns:
        return ValidationResult(
            passed=False,
            check_name="value_range",
            message=f"Column '{column}' not found in results",
            details={"column": column, "available_columns": result.columns},
        )

    values = [row.get(column) for row in result.rows if row.get(column) is not None]

    if not values:
        return ValidationResult(
            passed=True,
            check_name="value_range",
            message=f"Column '{column}' has no non-null values to check",
            details={"column": column, "value_count": 0},
        )

    violations = []
    for i, val in enumerate(values):
        try:
            num_val = float(val)
            if min_value is not None and num_val < min_value:
                violations.append({"row": i, "value": val, "issue": f"< {min_value}"})
            if max_value is not None and num_val > max_value:
                violations.append({"row": i, "value": val, "issue": f"> {max_value}"})
        except (ValueError, TypeError):
            violations.append({"row": i, "value": val, "issue": "not numeric"})

    if not violations:
        actual_min = min(float(v) for v in values)
        actual_max = max(float(v) for v in values)
        return ValidationResult(
            passed=True,
            check_name="value_range",
            message=f"All {len(values)} values in '{column}' within range",
            details={
                "column": column,
                "value_count": len(values),
                "actual_min": actual_min,
                "actual_max": actual_max,
                "expected_min": min_value,
                "expected_max": max_value,
            },
        )
    else:
        return ValidationResult(
            passed=False,
            check_name="value_range",
            message=f"{len(violations)} values in '{column}' out of range",
            details={
                "column": column,
                "violations": violations[:10],  # Limit to first 10
                "total_violations": len(violations),
                "expected_min": min_value,
                "expected_max": max_value,
            },
        )


def validate_no_nulls(
    result: ExecutionResult,
    columns: list[str],
) -> ValidationResult:
    """
    Check that specified columns have no null values.

    Args:
        result: ExecutionResult from query execution.
        columns: List of column names that should not contain nulls.

    Returns:
        ValidationResult indicating pass/fail.
    """
    null_counts: dict[str, int] = {}

    for col in columns:
        if col not in result.columns:
            null_counts[col] = -1  # Column missing
            continue

        null_count = sum(1 for row in result.rows if row.get(col) is None)
        if null_count > 0:
            null_counts[col] = null_count

    if not null_counts:
        return ValidationResult(
            passed=True,
            check_name="no_nulls",
            message=f"No null values in {len(columns)} checked columns",
            details={"columns": columns},
        )
    else:
        missing_cols = [c for c, n in null_counts.items() if n == -1]
        null_cols = {c: n for c, n in null_counts.items() if n > 0}

        issues = []
        if missing_cols:
            issues.append(f"missing columns: {missing_cols}")
        if null_cols:
            issues.append(f"null values: {null_cols}")

        return ValidationResult(
            passed=False,
            check_name="no_nulls",
            message=f"Null check failed: {'; '.join(issues)}",
            details={
                "missing_columns": missing_cols,
                "null_counts": null_cols,
            },
        )


def run_all_validations(
    result: ExecutionResult,
    expected_columns: list[str] | None = None,
    min_rows: int = 1,
    value_ranges: dict[str, tuple[float | None, float | None]] | None = None,
    non_null_columns: list[str] | None = None,
) -> list[ValidationResult]:
    """
    Run a standard set of validations on query results.

    Args:
        result: ExecutionResult from query execution.
        expected_columns: Optional list of columns that should be present.
        min_rows: Minimum number of rows expected (default 1).
        value_ranges: Optional dict mapping column names to (min, max) tuples.
        non_null_columns: Optional list of columns that should not have nulls.

    Returns:
        List of ValidationResult objects for each check performed.
    """
    validations = []

    # Always check execution success
    validations.append(validate_execution_success(result))

    # Check non-empty (using min_rows)
    validations.append(validate_min_rows(result, min_rows))

    # Check columns if specified
    if expected_columns:
        validations.append(validate_columns(result, expected_columns))

    # Check value ranges if specified
    if value_ranges:
        for col, (min_val, max_val) in value_ranges.items():
            validations.append(validate_value_in_range(result, col, min_val, max_val))

    # Check for nulls if specified
    if non_null_columns:
        validations.append(validate_no_nulls(result, non_null_columns))

    return validations
