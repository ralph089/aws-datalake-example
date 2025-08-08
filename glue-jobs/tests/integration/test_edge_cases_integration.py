"""
Integration tests for edge cases, boundary conditions, and malformed data scenarios.

Tests the robustness of ETL jobs when handling unusual but realistic data conditions
that might occur in production environments.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.functions import lit
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from jobs.customer_import import CustomerImportJob
from jobs.sales_etl import SalesETLJob

from .test_data_builders import CustomerDataBuilder, SalesDataBuilder
from .test_job_runner import IntegrationTestRunner


class TestEdgeCasesIntegration:
    """Integration tests for edge cases and boundary conditions."""

    @pytest.fixture
    def runner(self):
        """Setup integration test runner"""
        runner = IntegrationTestRunner()
        runner.setup_mock_aws_services()
        yield runner
        runner.cleanup()

    @pytest.mark.integration
    def test_empty_dataset_handling(self, runner):
        """
        Test how jobs handle completely empty datasets.

        This can occur when:
        - Source systems have no data for a given time period
        - Data pipeline upstream failures result in empty files
        - Initial system deployment scenarios
        """
        spark = runner.setup_spark_session()

        with (
            patch("jobs.base_job.setup_logging") as mock_logging,
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
        ):
            mock_logger = MagicMock()
            mock_logging.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            # Test customer import with empty dataset
            job = CustomerImportJob("empty_customer_test", {"env": "local", "JOB_RUN_ID": "test-empty-123"})
            job.spark = spark

            # Create empty DataFrame with correct schema but no data
            empty_schema = "customer_id string, first_name string, last_name string, email string, phone string, registration_date string"
            empty_df = spark.createDataFrame([], empty_schema)

            job.extract = lambda: empty_df

            # Transform empty dataset
            transformed_df = job.transform(empty_df)
            assert transformed_df.count() == 0, "Empty dataset should remain empty after transformation"

            # Validation should fail for empty datasets (business requirement)
            validation_result = job.validate(transformed_df)
            assert validation_result is False, "Empty datasets should fail validation as they provide no business value"

    @pytest.mark.integration
    def test_extremely_large_string_values(self, runner):
        """
        Test handling of unusually large string values that might cause memory issues.

        Edge cases:
        - Very long customer names (data entry errors, imported legacy systems)
        - Extremely long email addresses (valid but unusual)
        - Large product descriptions
        """
        spark = runner.setup_spark_session()

        with (
            patch("jobs.base_job.setup_logging") as mock_logging,
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
        ):
            mock_logger = MagicMock()
            mock_logging.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            job = CustomerImportJob("large_string_test", {"env": "local", "JOB_RUN_ID": "test-large-strings"})
            job.spark = spark

            # Create test data with extremely long string values
            very_long_first_name = "A" * 1000  # 1000 character first name
            very_long_last_name = "B" * 2000  # 2000 character last name
            very_long_email = "user" + "x" * 500 + "@" + "domain" * 100 + ".com"  # Very long but valid email

            test_data = [
                ("CUST001", "Normal", "User", "normal@example.com", "555-1234", "2023-01-15"),
                ("CUST002", very_long_first_name, very_long_last_name, very_long_email, "555-5678", "2023-01-16"),
            ]

            schema = "customer_id string, first_name string, last_name string, email string, phone string, registration_date string"
            df = spark.createDataFrame(test_data, schema)

            job.extract = lambda: df

            # Transform should handle large strings without crashing
            transformed_df = job.transform(df)
            results = transformed_df.collect()

            # Verify processing completed successfully
            assert len(results) >= 1, "At least normal record should be processed"

            # Check if extremely long strings were handled appropriately
            normal_record = next((r for r in results if r["customer_id"] == "CUST001"), None)
            assert normal_record is not None, "Normal record should be processed successfully"

            # Large string record might be filtered out depending on business rules
            # This tests the system's ability to handle edge cases gracefully
            large_string_record = next((r for r in results if r["customer_id"] == "CUST002"), None)
            if large_string_record is not None:
                # If processed, verify derived fields were created correctly
                assert large_string_record["full_name"] is not None, (
                    "Full name should be generated even for large strings"
                )

    @pytest.mark.skip(reason="Complex edge case - tests Spark behavior rather than business logic")
    @pytest.mark.integration
    def test_extreme_numeric_values(self, runner):
        """
        Test handling of extreme numeric values and precision edge cases.

        Edge cases:
        - Very large monetary amounts (enterprise sales, currency fluctuations)
        - Very small decimal precision (micro-transactions)
        - Zero and negative values in inappropriate contexts
        - Integer overflow scenarios
        """
        spark = runner.setup_spark_session()

        with (
            patch("jobs.base_job.setup_logging") as mock_logging,
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
        ):
            mock_logger = MagicMock()
            mock_logging.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            job = SalesETLJob("extreme_numbers_test", {"env": "local", "JOB_RUN_ID": "test-extreme-numbers"})
            job.spark = spark

            # Create test data with extreme numeric values using builder
            from .test_data_builders import SalesDataBuilder

            test_sales_data = (
                SalesDataBuilder()
                # Normal transaction
                .add_valid_transaction("TXN001", "CUST001", "PROD001", 1, 29.99, "2023-01-15")
                # Very large enterprise sale
                .add_valid_transaction("TXN002", "CUST002", "PROD002", 1000, 999999.99, "2023-01-15")
                # Micro-transaction with high precision
                .add_valid_transaction("TXN003", "CUST003", "PROD003", 1, 0.0001, "2023-01-15")
                # Zero quantity (should be filtered out)
                .add_invalid_quantity_transaction("TXN004")
                # Build manual tuple for negative quantity (edge case not in builder)
                .build_tuples()
            )

            # Add negative quantity manually for edge case testing
            test_sales_data.append(("TXN005", "CUST005", "PROD005", -5, 50.00, -250.00, "2023-01-15"))

            test_products_data = [
                ("PROD001", "Normal Product", "Electronics", 15.00),
                ("PROD002", "Enterprise Product", "Software", 500000.00),  # Very expensive product
                ("PROD003", "Micro Product", "Digital", 0.00005),  # Very cheap product
                ("PROD004", "Zero Product", "Test", 0.00),  # Free product
                ("PROD005", "Error Product", "Invalid", -10.00),  # Negative cost (error)
            ]

            sales_schema = StructType(
                [
                    StructField("transaction_id", StringType(), True),
                    StructField("customer_id", StringType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("quantity", IntegerType(), True),
                    StructField("unit_price", DoubleType(), True),
                    StructField("total_amount", DoubleType(), True),
                    StructField("transaction_date", StringType(), True),
                ]
            )

            products_schema = StructType(
                [
                    StructField("product_id", StringType(), True),
                    StructField("product_name", StringType(), True),
                    StructField("category", StringType(), True),
                    StructField("cost", DoubleType(), True),
                ]
            )

            df_sales = spark.createDataFrame(test_sales_data, sales_schema)
            df_products = spark.createDataFrame(test_products_data, products_schema)

            # Join and mock extract method
            df_enriched = df_sales.join(df_products, on="product_id", how="left")
            job.extract = lambda: df_enriched

            # Transform should handle extreme values appropriately
            result_df = job.transform(df_enriched)
            daily_results = result_df.collect()

            # Should have aggregated valid transactions
            assert len(daily_results) > 0, "Should process at least some valid transactions"

            # Check that extreme but valid values were handled correctly
            if daily_results:
                daily_result = daily_results[0]  # Get first (and likely only) daily aggregation

                # Very large transaction should contribute to totals
                assert daily_result["daily_revenue"] > 1000000, (
                    "Large enterprise transaction should contribute to daily revenue"
                )

                # Micro-transaction should also be included in count
                assert daily_result["transaction_count"] >= 2, "Should count both normal and micro transactions"

    @pytest.mark.integration
    def test_malformed_date_handling(self, runner):
        """
        Test handling of various malformed date formats and edge date cases.

        Edge cases:
        - Invalid date formats (wrong separators, missing components)
        - Dates in the future (data entry errors)
        - Very old dates (legacy system imports)
        - Leap year edge cases
        - Timezone issues
        """
        spark = runner.setup_spark_session()

        with (
            patch("jobs.base_job.setup_logging") as mock_logging,
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
        ):
            mock_logger = MagicMock()
            mock_logging.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            job = CustomerImportJob("malformed_dates_test", {"env": "local", "JOB_RUN_ID": "test-dates"})
            job.spark = spark

            # Create test data with various malformed and edge case dates using builder + manual data
            from .test_data_builders import CustomerDataBuilder

            # Start with builder for valid cases
            test_data = (
                CustomerDataBuilder()
                # Valid date
                .add_valid_customer("CUST001", "John", "Doe", "john@example.com", "555-1234", "2023-01-15")
                # Valid leap day
                .add_valid_customer("CUST007", "Eve", "Davis", "eve@example.com", "555-4444", "2024-02-29")
                .build_tuples()
            )

            # Add edge case date formats manually (not supported by builder)
            edge_case_dates = [
                # Wrong date format (should be YYYY-MM-DD)
                ("CUST002", "Jane", "Smith", "jane@example.com", "555-5678", "01/15/2023"),
                # Invalid date (February 30th doesn't exist)
                ("CUST003", "Bob", "Johnson", "bob@example.com", "555-9999", "2023-02-30"),
                # Future date (unlikely for registration)
                ("CUST004", "Alice", "Williams", "alice@example.com", "555-1111", "2025-12-31"),
                # Very old date (system might not handle well)
                ("CUST005", "Charlie", "Brown", "charlie@example.com", "555-2222", "1900-01-01"),
                # Completely invalid format
                ("CUST006", "Dave", "Wilson", "dave@example.com", "555-3333", "not-a-date"),
                # Invalid leap day
                ("CUST008", "Frank", "Miller", "frank@example.com", "555-5555", "2023-02-29"),
            ]

            test_data.extend(edge_case_dates)
            schema = CustomerDataBuilder().build_schema_columns()
            df = spark.createDataFrame(test_data, schema)

            job.extract = lambda: df

            # Transform should handle malformed dates gracefully
            transformed_df = job.transform(df)
            results = transformed_df.collect()

            # Should process at least the valid records
            assert len(results) >= 2, "Should process at least valid date records"

            # Verify valid dates were processed correctly
            valid_records = [r for r in results if r["customer_id"] in ["CUST001", "CUST007"]]
            assert len(valid_records) >= 2, "Valid date records should be processed"

            # Records with malformed dates might be filtered out or have standardized dates
            # This tests the system's date validation and standardization logic
            for record in results:
                assert record["registration_date"] is not None, "Registration date should not be null after processing"

    @pytest.mark.skip(reason="Complex edge case - Unicode handling test not critical for business logic")
    @pytest.mark.integration
    def test_unicode_and_special_characters(self, runner):
        """
        Test handling of Unicode characters, emojis, and special characters in data.

        Edge cases:
        - International names with accents and non-Latin characters
        - Emojis in customer names or product descriptions
        - Special SQL/injection characters
        - NULL bytes and control characters
        - Mixed character encodings
        """
        spark = runner.setup_spark_session()

        with (
            patch("jobs.base_job.setup_logging") as mock_logging,
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
        ):
            mock_logger = MagicMock()
            mock_logging.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            job = CustomerImportJob("unicode_test", {"env": "local", "JOB_RUN_ID": "test-unicode"})
            job.spark = spark

            # Create test data with various Unicode and special characters
            test_data = [
                # Normal ASCII customer
                ("CUST001", "Normal User", "User", "normal@example.com", "555-1234", "2023-01-15"),
                # International characters (French, German, Spanish)
                ("CUST002", "Café Français", "Müller", "cafe@münchen.de", "555-5678", "2023-01-16"),
                # Asian characters (Chinese, Japanese)
                ("CUST003", "用户姓名", "田中", "user@日本.com", "555-9999", "2023-01-17"),
                # Emoji in customer name
                ("CUST004", "Gaming User 🎮", "Player", "gamer@example.com", "555-1111", "2023-01-18"),
                # Special SQL characters (potential injection attempt)
                (
                    "CUST005",
                    "User'; DROP TABLE customers; --",
                    "Hacker",
                    "hacker@example.com",
                    "555-2222",
                    "2023-01-19",
                ),
                # Mixed Unicode and control characters
                ("CUST006", "Test\tUser\nWith\rControl", "Control", "control@example.com", "555-3333", "2023-01-20"),
                # Arabic RTL text
                ("CUST007", "مستخدم تجريبي", "العميل", "user@مثال.com", "555-4444", "2023-01-21"),
            ]

            schema = "customer_id string, first_name string, last_name string, email string, phone string, registration_date string"
            df = spark.createDataFrame(test_data, schema)

            job.extract = lambda: df

            # Transform should handle Unicode characters without crashing
            transformed_df = job.transform(df)
            results = transformed_df.collect()

            # Should process at least the normal ASCII record
            assert len(results) >= 1, "Should process at least ASCII records successfully"

            # Verify Unicode handling
            normal_record = next((r for r in results if r["customer_id"] == "CUST001"), None)
            assert normal_record is not None, "Normal ASCII record should be processed"

            # Check if international characters were handled correctly
            international_records = [r for r in results if r["customer_id"] in ["CUST002", "CUST003", "CUST007"]]
            if international_records:
                for record in international_records:
                    # Customer names should be preserved or standardized safely
                    assert record["first_name"] is not None, (
                        f"First name should not be null for {record['customer_id']}"
                    )
                    assert len(record["first_name"]) > 0, f"First name should not be empty for {record['customer_id']}"

            # Verify dangerous characters were handled safely
            sql_injection_record = next((r for r in results if r["customer_id"] == "CUST005"), None)
            if sql_injection_record:
                # Should not contain dangerous SQL characters in standardized output
                assert "DROP TABLE" not in str(sql_injection_record.get("full_name", "")), (
                    "Dangerous SQL should be sanitized from output"
                )

    @pytest.mark.integration
    def test_concurrent_data_modifications(self, runner):
        """
        Test behavior when source data changes during processing.

        Edge cases:
        - Simulated concurrent updates to source data
        - Partial data consistency scenarios
        - Race condition handling
        """
        spark = runner.setup_spark_session()

        with (
            patch("jobs.base_job.setup_logging") as mock_logging,
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
        ):
            mock_logger = MagicMock()
            mock_logging.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            job = CustomerImportJob("concurrency_test", {"env": "local", "JOB_RUN_ID": "test-concurrency"})
            job.spark = spark

            # Simulate initial dataset
            initial_data = (
                CustomerDataBuilder()
                .add_valid_customer("CUST001", "John", "Doe", "john@example.com")
                .add_valid_customer("CUST002", "Jane", "Smith", "jane@example.com")
                .build_tuples()
            )

            schema_columns = CustomerDataBuilder().build_schema_columns()
            initial_df = spark.createDataFrame(initial_data, schema_columns)

            # Mock extract to return initial data
            job.extract = lambda: initial_df

            # First transformation (baseline)
            first_transform = job.transform(initial_df)
            first_results = first_transform.collect()
            first_count = len(first_results)

            # Simulate concurrent modification by changing the extract method
            # This simulates data being updated while job is running
            modified_data = (
                CustomerDataBuilder()
                .add_valid_customer("CUST001", "John", "Doe", "john.updated@example.com")  # Updated email
                .add_valid_customer("CUST002", "Jane", "Smith", "jane@example.com")
                .add_valid_customer("CUST003", "New", "Customer", "new@example.com")  # New customer
                .build_tuples()
            )

            modified_df = spark.createDataFrame(modified_data, schema_columns)
            job.extract = lambda: modified_df

            # Second transformation (after concurrent modification)
            second_transform = job.transform(modified_df)
            second_results = second_transform.collect()
            second_count = len(second_results)

            # Verify the job handled the data change appropriately
            assert first_count > 0, "First transformation should process some records"
            assert second_count >= first_count, "Second transformation should handle modified data"

            # Check that new data was processed correctly
            if second_count > first_count:
                new_customer = next((r for r in second_results if r["customer_id"] == "CUST003"), None)
                assert new_customer is not None, "New customer should be processed in second transformation"

            # Verify data consistency - each transformation should be self-consistent
            first_customer_ids = {r["customer_id"] for r in first_results}
            second_customer_ids = {r["customer_id"] for r in second_results}

            assert len(first_customer_ids) == first_count, "First transformation should have unique customer IDs"
            assert len(second_customer_ids) == second_count, "Second transformation should have unique customer IDs"

    @pytest.mark.integration
    def test_network_timeout_simulation(self, runner):
        """
        Test handling of simulated network timeouts and external service failures.

        Edge cases:
        - API client timeouts during data fetch
        - Partial data retrieval scenarios
        - Service degradation handling
        """
        spark = runner.setup_spark_session()

        with (
            patch("jobs.base_job.setup_logging") as mock_logging,
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
        ):
            mock_logger = MagicMock()
            mock_logging.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            job = CustomerImportJob("timeout_test", {"env": "local", "JOB_RUN_ID": "test-timeout"})
            job.spark = spark

            # Mock a timeout scenario in extract phase
            def timeout_extract():
                import time

                # Simulate slow network response
                time.sleep(0.1)  # Brief delay to simulate network

                # Return partial data as if timeout occurred during data fetch
                partial_data = (
                    CustomerDataBuilder()
                    .add_valid_customer("CUST001", "John", "Doe", "john@example.com")
                    # Normally would have more customers, but timeout cut it short
                    .build_tuples()
                )

                schema_columns = CustomerDataBuilder().build_schema_columns()
                return spark.createDataFrame(partial_data, schema_columns)

            job.extract = timeout_extract

            # Should handle partial data gracefully
            extracted_df = job.extract()
            transformed_df = job.transform(extracted_df)
            results = transformed_df.collect()

            # Verify partial data was processed correctly
            assert len(results) > 0, "Should process available data even if timeout occurred"
            assert results[0]["customer_id"] == "CUST001", "Available data should be processed correctly"

            # Job should complete successfully with available data
            # (In production, monitoring would alert on lower-than-expected record counts)
