"""
Simplified data quality checker without Great Expectations complexity.
Provides basic data validation functions for AWS Glue ETL jobs.
"""

from typing import Any

import structlog
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan

logger = structlog.get_logger()


class SimpleDataQualityChecker:
    """Simple data quality checker with basic validation rules."""

    def __init__(self):
        self.validation_results = []

    def check_completeness(self, df: DataFrame, required_columns: list[str]) -> dict[str, Any]:
        """Check that required columns have no null values."""
        results = {}
        total_rows = df.count()

        for column in required_columns:
            if column not in df.columns:
                results[column] = {
                    "status": "FAILED",
                    "message": f"Column '{column}' not found in dataframe",
                    "null_count": None,
                    "null_percentage": None,
                }
                continue

            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0

            results[column] = {
                "status": "PASSED" if null_count == 0 else "FAILED",
                "message": f"Column '{column}' has {null_count} null values ({null_percentage:.2f}%)",
                "null_count": null_count,
                "null_percentage": null_percentage,
            }

        return results

    def check_uniqueness(self, df: DataFrame, columns: list[str]) -> dict[str, Any]:
        """Check that specified columns have unique values."""
        results = {}
        total_rows = df.count()

        for column in columns:
            if column not in df.columns:
                results[column] = {
                    "status": "FAILED",
                    "message": f"Column '{column}' not found in dataframe",
                    "unique_count": None,
                    "duplicate_count": None,
                }
                continue

            unique_count = df.select(column).distinct().count()
            duplicate_count = total_rows - unique_count

            results[column] = {
                "status": "PASSED" if duplicate_count == 0 else "FAILED",
                "message": f"Column '{column}' has {duplicate_count} duplicate values",
                "unique_count": unique_count,
                "duplicate_count": duplicate_count,
            }

        return results

    def check_numeric_range(
        self, df: DataFrame, column: str, min_val: float = None, max_val: float = None
    ) -> dict[str, Any]:
        """Check that numeric column values are within specified range."""
        if column not in df.columns:
            return {"status": "FAILED", "message": f"Column '{column}' not found in dataframe"}

        out_of_range_count = 0
        total_rows = df.count()

        if min_val is not None:
            out_of_range_count += df.filter(col(column) < min_val).count()

        if max_val is not None:
            out_of_range_count += df.filter(col(column) > max_val).count()

        return {
            "status": "PASSED" if out_of_range_count == 0 else "FAILED",
            "message": f"Column '{column}' has {out_of_range_count} values outside range [{min_val}, {max_val}]",
            "out_of_range_count": out_of_range_count,
            "total_rows": total_rows,
        }

    def check_string_pattern(self, df: DataFrame, column: str, pattern: str) -> dict[str, Any]:
        """Check that string column values match a regex pattern."""
        if column not in df.columns:
            return {"status": "FAILED", "message": f"Column '{column}' not found in dataframe"}

        invalid_count = df.filter(~col(column).rlike(pattern)).count()
        total_rows = df.count()

        return {
            "status": "PASSED" if invalid_count == 0 else "FAILED",
            "message": f"Column '{column}' has {invalid_count} values not matching pattern '{pattern}'",
            "invalid_count": invalid_count,
            "total_rows": total_rows,
        }

    def validate_dataframe(self, df: DataFrame, validation_rules: dict[str, Any]) -> dict[str, Any]:
        """Run multiple validation rules on a dataframe."""
        results = {
            "overall_status": "PASSED",
            "validation_count": 0,
            "passed_count": 0,
            "failed_count": 0,
            "validations": {},
        }

        # Check completeness
        if "required_columns" in validation_rules:
            completeness_results = self.check_completeness(df, validation_rules["required_columns"])
            results["validations"]["completeness"] = completeness_results

            for column, result in completeness_results.items():
                results["validation_count"] += 1
                if result["status"] == "PASSED":
                    results["passed_count"] += 1
                else:
                    results["failed_count"] += 1
                    results["overall_status"] = "FAILED"

        # Check uniqueness
        if "unique_columns" in validation_rules:
            uniqueness_results = self.check_uniqueness(df, validation_rules["unique_columns"])
            results["validations"]["uniqueness"] = uniqueness_results

            for column, result in uniqueness_results.items():
                results["validation_count"] += 1
                if result["status"] == "PASSED":
                    results["passed_count"] += 1
                else:
                    results["failed_count"] += 1
                    results["overall_status"] = "FAILED"

        # Check numeric ranges
        if "numeric_ranges" in validation_rules:
            for column, range_spec in validation_rules["numeric_ranges"].items():
                range_result = self.check_numeric_range(df, column, range_spec.get("min"), range_spec.get("max"))
                if "numeric_ranges" not in results["validations"]:
                    results["validations"]["numeric_ranges"] = {}
                results["validations"]["numeric_ranges"][column] = range_result

                results["validation_count"] += 1
                if range_result["status"] == "PASSED":
                    results["passed_count"] += 1
                else:
                    results["failed_count"] += 1
                    results["overall_status"] = "FAILED"

        # Check string patterns
        if "string_patterns" in validation_rules:
            for column, pattern in validation_rules["string_patterns"].items():
                pattern_result = self.check_string_pattern(df, column, pattern)
                if "string_patterns" not in results["validations"]:
                    results["validations"]["string_patterns"] = {}
                results["validations"]["string_patterns"][column] = pattern_result

                results["validation_count"] += 1
                if pattern_result["status"] == "PASSED":
                    results["passed_count"] += 1
                else:
                    results["failed_count"] += 1
                    results["overall_status"] = "FAILED"

        logger.info(
            "data_quality_validation_completed",
            overall_status=results["overall_status"],
            total_validations=results["validation_count"],
            passed=results["passed_count"],
            failed=results["failed_count"],
        )

        return results
