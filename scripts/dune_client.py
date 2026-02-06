"""
Dune Analytics API client wrapper.

Provides a simplified interface for executing queries and retrieving results
using the official dune-client SDK.
"""

import os
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dune_client.types import QueryParameter


@dataclass
class ExecutionResult:
    """Result of a query execution."""

    success: bool
    execution_id: str | None
    state: str
    rows: list[dict[str, Any]]
    columns: list[str]
    row_count: int
    error: str | None = None
    execution_time_ms: int | None = None

    @property
    def is_empty(self) -> bool:
        """Check if result has no rows."""
        return self.row_count == 0


def get_client() -> DuneClient:
    """
    Initialize and return a Dune API client.

    Loads API key from environment variable DUNE_API_KEY.
    Searches for .env file in current directory and parent directories.

    Returns:
        DuneClient: Initialized Dune API client.

    Raises:
        ValueError: If DUNE_API_KEY environment variable is not set.
    """
    load_dotenv()

    api_key = os.getenv("DUNE_API_KEY")
    if not api_key:
        raise ValueError(
            "DUNE_API_KEY environment variable is not set. "
            "Copy .env.example to .env and add your API key."
        )

    return DuneClient(api_key)


def execute_sql(
    sql: str,
    params: dict[str, Any] | None = None,
    timeout_seconds: int = 300,
) -> ExecutionResult:
    """
    Execute raw SQL query via Dune API.

    This is the primary method for smoke testing local query changes
    before syncing to Dune.

    Args:
        sql: SQL query string to execute.
        params: Optional dictionary of query parameters.
        timeout_seconds: Maximum time to wait for query completion.

    Returns:
        ExecutionResult with query results or error information.
    """
    client = get_client()

    # Convert params dict to QueryParameter list if provided
    query_params = []
    if params:
        for key, value in params.items():
            query_params.append(QueryParameter.text_type(key, str(value)))

    try:
        # Execute and wait for results
        result = client.run_sql(
            query_sql=sql,
            query_parameters=query_params if query_params else None,
        )

        # Extract result data
        rows = result.result.rows if result.result else []
        columns = []
        if rows:
            columns = list(rows[0].keys())

        return ExecutionResult(
            success=True,
            execution_id=result.execution_id,
            state=str(result.state),
            rows=rows,
            columns=columns,
            row_count=len(rows),
            execution_time_ms=result.execution_ended_at.timestamp() * 1000
            if result.execution_ended_at
            else None,
        )

    except Exception as e:
        return ExecutionResult(
            success=False,
            execution_id=None,
            state="FAILED",
            rows=[],
            columns=[],
            row_count=0,
            error=str(e),
        )


def execute_query(
    query_id: int,
    params: dict[str, Any] | None = None,
    timeout_seconds: int = 300,
) -> ExecutionResult:
    """
    Execute a saved Dune query by ID.

    Use this when you want to run a query that's already saved on Dune.

    Args:
        query_id: Dune query ID (numeric).
        params: Optional dictionary of query parameters.
        timeout_seconds: Maximum time to wait for query completion.

    Returns:
        ExecutionResult with query results or error information.
    """
    client = get_client()

    # Convert params dict to QueryParameter list if provided
    query_params = []
    if params:
        for key, value in params.items():
            query_params.append(QueryParameter.text_type(key, str(value)))

    try:
        # Create query reference and execute
        query = QueryBase(query_id=query_id)
        result = client.run_query(
            query=query,
            query_parameters=query_params if query_params else None,
        )

        # Extract result data
        rows = result.result.rows if result.result else []
        columns = []
        if rows:
            columns = list(rows[0].keys())

        return ExecutionResult(
            success=True,
            execution_id=result.execution_id,
            state=str(result.state),
            rows=rows,
            columns=columns,
            row_count=len(rows),
            execution_time_ms=result.execution_ended_at.timestamp() * 1000
            if result.execution_ended_at
            else None,
        )

    except Exception as e:
        return ExecutionResult(
            success=False,
            execution_id=None,
            state="FAILED",
            rows=[],
            columns=[],
            row_count=0,
            error=str(e),
        )


def get_latest_result(
    query_id: int,
    max_age_hours: int = 8,
) -> ExecutionResult:
    """
    Get the latest cached result for a saved query.

    This avoids re-execution costs if a recent result exists.

    Args:
        query_id: Dune query ID (numeric).
        max_age_hours: Maximum age of cached result to accept.

    Returns:
        ExecutionResult with cached results or error if too old/missing.
    """
    client = get_client()

    try:
        query = QueryBase(query_id=query_id)
        result = client.get_latest_result(
            query=query,
            max_age_hours=max_age_hours,
        )

        # Extract result data
        rows = result.result.rows if result.result else []
        columns = []
        if rows:
            columns = list(rows[0].keys())

        return ExecutionResult(
            success=True,
            execution_id=result.execution_id,
            state=str(result.state),
            rows=rows,
            columns=columns,
            row_count=len(rows),
        )

    except Exception as e:
        return ExecutionResult(
            success=False,
            execution_id=None,
            state="FAILED",
            rows=[],
            columns=[],
            row_count=0,
            error=str(e),
        )
