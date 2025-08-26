"""
Data Lake connector for querying Iceberg tables via Amazon Athena.

Provides functionality to execute queries, retrieve results, and format data for API transmission.
"""

import json
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from config import LambdaConfig


class DataLakeConnector:
    """Connects to AWS Data Lake through Athena to query Iceberg tables."""

    def __init__(self, config: LambdaConfig):
        """Initialize connector with AWS clients."""
        self.config = config
        self.athena_client = boto3.client("athena")
        self.s3_client = boto3.client("s3")

    def query_table(self, query: str) -> list[dict[str, Any]]:
        """
        Execute Athena query and return results as list of dictionaries.

        Args:
            query: SQL query to execute

        Returns:
            List of records as dictionaries

        Raises:
            RuntimeError: If query fails or times out
        """
        try:
            # Start query execution
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": self.config.athena_database},
                WorkGroup=self.config.athena_workgroup,
                ResultConfiguration={"OutputLocation": f"s3://{self.config.athena_output_bucket}/lambda-queries/"},
            )

            query_execution_id = response["QueryExecutionId"]

            # Wait for query completion
            self._wait_for_query_completion(query_execution_id)

            # Get results
            return self._get_query_results(query_execution_id)

        except (ClientError, NoCredentialsError) as e:
            raise RuntimeError(f"Failed to execute Athena query: {e}") from e

    def build_query(
        self, table_name: str, date_filter: str = "yesterday", where_clause: str | None = None, limit: int | None = None
    ) -> str:
        """
        Build SQL query for Iceberg table.

        Args:
            table_name: Name of the Iceberg table
            date_filter: Date filter (today, yesterday, last_week)
            where_clause: Additional WHERE conditions
            limit: Maximum number of records

        Returns:
            SQL query string
        """
        # Base query
        query = f"SELECT * FROM {table_name}"

        # Date filter conditions
        date_conditions = {
            "today": "DATE(processed_timestamp) = CURRENT_DATE",
            "yesterday": "DATE(processed_timestamp) = CURRENT_DATE - INTERVAL '1' DAY",
            "last_week": "DATE(processed_timestamp) >= CURRENT_DATE - INTERVAL '7' DAY",
        }

        conditions = []

        # Add date filter if available
        if date_filter in date_conditions:
            conditions.append(date_conditions[date_filter])

        # Add custom where clause
        if where_clause:
            conditions.append(where_clause)

        # Build WHERE clause
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Add limit
        if limit:
            query += f" LIMIT {min(limit, self.config.max_records)}"

        return query

    def _wait_for_query_completion(self, query_execution_id: str, timeout: int = 300) -> None:
        """
        Wait for Athena query to complete.

        Args:
            query_execution_id: Athena query execution ID
            timeout: Maximum wait time in seconds

        Raises:
            RuntimeError: If query fails or times out
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)

            status = response["QueryExecution"]["Status"]["State"]

            if status == "SUCCEEDED":
                return
            elif status in ["FAILED", "CANCELLED"]:
                reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
                raise RuntimeError(f"Athena query failed: {reason}")

            time.sleep(2)

        raise RuntimeError(f"Athena query timed out after {timeout} seconds")

    def _get_query_results(self, query_execution_id: str) -> list[dict[str, Any]]:
        """
        Get query results from Athena.

        Args:
            query_execution_id: Athena query execution ID

        Returns:
            List of records as dictionaries
        """
        results = []
        paginator = self.athena_client.get_paginator("get_query_results")

        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            rows = page["ResultSet"]["Rows"]

            # Skip header row on first page
            if not results and rows:
                headers = [col["VarCharValue"] for col in rows[0]["Data"]]
                rows = rows[1:]
            elif not results:
                continue

            # Convert rows to dictionaries
            for row in rows:
                record = {}
                for i, col in enumerate(row["Data"]):
                    # Handle different data types
                    value = col.get("VarCharValue")
                    if value is not None:
                        # Try to parse as JSON for complex types
                        try:
                            if value.startswith(("{", "[")):
                                value = json.loads(value)
                        except json.JSONDecodeError:
                            pass

                    record[headers[i]] = value

                results.append(record)

                # Respect max records limit
                if len(results) >= self.config.max_records:
                    break

            if len(results) >= self.config.max_records:
                break

        return results

    def get_table_info(self, table_name: str) -> dict[str, Any]:
        """
        Get basic information about an Iceberg table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table information
        """
        query = f"DESCRIBE {table_name}"

        try:
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": self.config.athena_database},
                WorkGroup=self.config.athena_workgroup,
                ResultConfiguration={"OutputLocation": f"s3://{self.config.athena_output_bucket}/lambda-queries/"},
            )

            query_execution_id = response["QueryExecutionId"]
            self._wait_for_query_completion(query_execution_id, timeout=60)

            columns = self._get_query_results(query_execution_id)

            return {
                "table_name": table_name,
                "column_count": len(columns),
                "columns": [{"name": col["col_name"], "type": col["data_type"]} for col in columns],
            }

        except Exception as e:
            return {"table_name": table_name, "error": str(e)}
