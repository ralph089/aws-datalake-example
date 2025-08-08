"""
Integration tests for SalesETLJob
Tests the complete ETL pipeline including extract, transform, validate, and load phases
with complex aggregations and multiple output tables.
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

from jobs.sales_etl import SalesETLJob

from .test_job_runner import IntegrationTestRunner


class TestSalesETLIntegration:
    """Integration tests for sales ETL job"""

    @pytest.fixture
    def runner(self):
        """Setup integration test runner"""
        runner = IntegrationTestRunner()
        runner.setup_mock_aws_services()
        yield runner
        runner.cleanup()

    @pytest.mark.integration
    def test_sales_etl_full_pipeline_success(self, runner):
        """Test successful execution of complete sales ETL pipeline"""
        # Verify test data files exist
        sales_data_path = Path(__file__).parent.parent / "test_data" / "sales_etl" / "sales.csv"
        products_data_path = Path(__file__).parent.parent / "test_data" / "sales_etl" / "products.csv"

        assert sales_data_path.exists(), f"Sales test data not found at {sales_data_path}"
        assert products_data_path.exists(), f"Products test data not found at {products_data_path}"

        # Test job execution
        result = runner.run_job_with_args(SalesETLJob, {"env": "local"})

        # Validate successful execution
        assert result["success"] is True, f"Job failed with error: {result.get('error')}"

        # Validate record counts
        assert result["raw_count"] > 0, "No raw records extracted"
        assert result["transformed_count"] > 0, "No records after transformation"
        assert result["final_count"] > 0, "No final records"

        # Validate expected aggregation columns are present

        # Check that aggregation-specific columns exist
        assert "daily_revenue" in result["columns"], "daily_revenue column missing from daily aggregation"
        assert "transaction_count" in result["columns"], "transaction_count column missing"
        assert "aggregation_type" in result["columns"], "aggregation_type column missing"

    @pytest.mark.integration
    def test_sales_etl_aggregation_logic(self, runner):
        """
        Test multi-dimensional sales aggregation logic with known data scenarios.

        This test validates the complete sales ETL transformation pipeline that creates
        three types of business aggregations from raw transaction data:
        1. Daily aggregations (total revenue, transaction count, quantity by day)
        2. Product aggregations (sales performance by product across days)
        3. Customer aggregations (customer purchase behavior by day)

        Test Data Design:
        - 4 transactions across 2 days (2023-01-15, 2023-01-16)
        - 2 customers (CUST001, CUST002) with different purchase patterns
        - 2 products (PROD001, PROD002) with different price points
        """
        spark = runner.setup_spark_session()

        # Create test sales data with carefully designed scenarios using builders
        from .test_data_builders import SalesDataBuilder

        sales_data = (
            SalesDataBuilder()
            # Day 1 (2023-01-15): Customer CUST001 makes 2 purchases
            .add_valid_transaction("TXN001", "CUST001", "PROD001", 2, 29.99, "2023-01-15")  # Headphones: 2 units
            .add_valid_transaction("TXN002", "CUST001", "PROD002", 1, 149.99, "2023-01-15")  # Watch: 1 unit
            # Day 2 (2023-01-16): Customer CUST002 makes 2 purchases
            .add_valid_transaction("TXN003", "CUST002", "PROD001", 3, 29.99, "2023-01-16")  # Headphones: 3 units
            .add_valid_transaction("TXN004", "CUST002", "PROD002", 1, 149.99, "2023-01-16")  # Watch: 1 unit
            .build_tuples()
        )

        # Create product reference data with cost information for profit calculation
        products_data = [
            ("PROD001", "Wireless Headphones", "Electronics", 15.00),  # Cost: $15.00, Profit margin varies
            ("PROD002", "Smart Watch", "Electronics", 75.00),  # Cost: $75.00, Higher cost product
        ]

        # Define explicit schemas for data type safety and validation
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

        df_sales = spark.createDataFrame(sales_data, sales_schema)
        df_products = spark.createDataFrame(products_data, products_schema)

        # Mock external job dependencies for isolated testing
        with (
            patch("jobs.base_job.setup_logging") as mock_logging,
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
        ):
            mock_logger = MagicMock()
            mock_logging.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            job = SalesETLJob("test_job", {"env": "local", "JOB_RUN_ID": "test-123"})
            job.spark = spark

            # Simulate extract phase: Join sales transactions with product master data
            # This enriches transactions with product names, categories, and cost for profit calculation
            df_enriched = df_sales.join(
                df_products.select("product_id", "product_name", "category", "cost"), on="product_id", how="left"
            )

            # Execute the transformation pipeline (creates multiple aggregation outputs)
            result_df = job.transform(df_enriched)
            daily_results = result_df.collect()

            # VALIDATION 1: Daily Aggregations Business Logic
            # Expected: 2 daily summaries (one per transaction date)
            assert len(daily_results) == 2, f"Expected 2 daily aggregations, got {len(daily_results)}"

            # Extract results by day for detailed validation
            day_15_result = next((r for r in daily_results if str(r["transaction_day"]) == "15"), None)
            day_16_result = next((r for r in daily_results if str(r["transaction_day"]) == "16"), None)

            assert day_15_result is not None, "Missing aggregation for 2023-01-15"
            assert day_16_result is not None, "Missing aggregation for 2023-01-16"

            # Validate 2023-01-15 daily aggregation (TXN001 + TXN002):
            # - Transaction count: 2 (TXN001, TXN002)
            # - Revenue: $59.98 + $149.99 = $209.97
            # - Quantity: 2 + 1 = 3 units
            assert day_15_result["transaction_count"] == 2, (
                f"Jan 15 transaction count: expected 2, got {day_15_result['transaction_count']}"
            )
            assert abs(day_15_result["daily_revenue"] - 209.97) < 0.01, (
                f"Jan 15 revenue: expected $209.97, got ${day_15_result['daily_revenue']}"
            )
            assert day_15_result["total_quantity"] == 3, (
                f"Jan 15 quantity: expected 3, got {day_15_result['total_quantity']}"
            )

            # Validate 2023-01-16 daily aggregation (TXN003 + TXN004):
            # - Transaction count: 2 (TXN003, TXN004)
            # - Revenue: $89.97 + $149.99 = $239.96
            # - Quantity: 3 + 1 = 4 units
            assert day_16_result["transaction_count"] == 2, (
                f"Jan 16 transaction count: expected 2, got {day_16_result['transaction_count']}"
            )
            assert abs(day_16_result["daily_revenue"] - 239.96) < 0.01, (
                f"Jan 16 revenue: expected $239.96, got ${day_16_result['daily_revenue']}"
            )
            assert day_16_result["total_quantity"] == 4, (
                f"Jan 16 quantity: expected 4, got {day_16_result['total_quantity']}"
            )

            # VALIDATION 2: Multi-dimensional Aggregation Architecture
            # Verify that the ETL job creates separate aggregation outputs for different business views
            assert hasattr(job, "daily_agg"), "Daily aggregation DataFrame not created/stored"
            assert hasattr(job, "product_agg"), "Product aggregation DataFrame not created/stored"
            assert hasattr(job, "customer_agg"), "Customer aggregation DataFrame not created/stored"

            # VALIDATION 3: Product Aggregations Business Logic
            # Expected: Product performance across all days
            # Should create combinations of (product_id, transaction_date) for sales analysis
            product_results = job.product_agg.collect()
            # Expected combinations: (PROD001,Jan15), (PROD002,Jan15), (PROD001,Jan16), (PROD002,Jan16)
            assert len(product_results) == 4, (
                f"Expected 4 product-day aggregations, got {len(product_results)} - "
                f"should have each product (PROD001, PROD002) for each day (Jan15, Jan16)"
            )

            # VALIDATION 4: Customer Aggregations Business Logic
            # Expected: Customer purchase behavior by day
            # CUST001: only purchased on Jan15, CUST002: only purchased on Jan16
            customer_results = job.customer_agg.collect()
            assert len(customer_results) == 2, (
                f"Expected 2 customer-day aggregations, got {len(customer_results)} - "
                f"CUST001 (Jan15 only) + CUST002 (Jan16 only)"
            )

            # Verify customer aggregation details
            customer_ids = {r["customer_id"] for r in customer_results}
            assert customer_ids == {
                "CUST001",
                "CUST002",
            }, f"Expected customers CUST001, CUST002 in aggregations, got {customer_ids}"

    @pytest.mark.integration
    def test_sales_etl_validation_failures(self, runner):
        """Test validation failure scenarios"""
        spark = runner.setup_spark_session()

        # Create data with validation issues (negative revenue)
        invalid_sales_data = [
            ("TXN001", "CUST001", "PROD001", 2, 29.99, -59.98, "2023-01-15"),  # Negative total_amount
            ("TXN002", "CUST002", "PROD002", 0, 149.99, 149.99, "2023-01-16"),  # Zero quantity
        ]

        schema = StructType(
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

        df = spark.createDataFrame(invalid_sales_data, schema)

        with (
            patch("jobs.base_job.setup_logging") as mock_logging,
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
        ):
            mock_logger = MagicMock()
            mock_logging.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            job = SalesETLJob("test_job", {"env": "local", "JOB_RUN_ID": "test-123"})
            job.spark = spark

            # Transform should filter out invalid records
            transformed_df = job.transform(df)

            # Should have no records after filtering
            assert transformed_df.count() == 0, "Invalid records should be filtered out"

            # But if we had valid records, validation could still fail for business rules
            # Let's create a scenario with zero transaction count per day (edge case)
            empty_agg_data = []
            empty_schema = StructType(
                [
                    StructField("transaction_date_trunc", StringType(), True),
                    StructField("transaction_count", IntegerType(), True),
                    StructField("daily_revenue", DoubleType(), True),
                ]
            )

            empty_df = spark.createDataFrame(empty_agg_data, empty_schema)
            validation_result = job.validate(empty_df)  # Use base validation for empty dataset

            # Should fail due to no records
            assert validation_result is False, "Validation should fail for empty aggregation"

    @pytest.mark.integration
    def test_sales_etl_product_join_missing(self, runner):
        """Test behavior when product data is not available"""
        spark = runner.setup_spark_session()

        # Create sales data only (no product data)
        sales_data = [
            ("TXN001", "CUST001", "PROD001", 2, 29.99, 59.98, "2023-01-15"),
        ]

        schema = StructType(
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

        df_sales = spark.createDataFrame(sales_data, schema)

        with (
            patch("jobs.base_job.setup_logging") as mock_logging,
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
        ):
            mock_logger = MagicMock()
            mock_logging.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            job = SalesETLJob("test_job", {"env": "local", "JOB_RUN_ID": "test-123"})
            job.spark = spark

            # Simulate extract without products data available
            # The job should handle missing product data gracefully
            result_df = job.transform(df_sales)
            results = result_df.collect()

            # Should still process successfully with just sales data
            assert len(results) == 1, "Should process sales data even without products"

            # Product-specific fields should be null/missing but processing should continue
            result = results[0]
            assert result["transaction_count"] == 1, "Transaction count should be correct"
            assert abs(result["daily_revenue"] - 59.98) < 0.01, "Revenue should be calculated from sales data"

    @pytest.mark.integration
    def test_sales_etl_local_file_outputs(self, runner):
        """Test that all three local output files are created correctly"""
        spark = runner.setup_spark_session()
        output_dir = runner.get_temp_dir()

        # Create minimal test data
        sales_data = [
            ("TXN001", "CUST001", "PROD001", 2, 29.99, 59.98, "2023-01-15"),
            ("TXN002", "CUST002", "PROD001", 1, 29.99, 29.99, "2023-01-16"),
        ]

        schema = StructType(
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

        df = spark.createDataFrame(sales_data, schema)

        with (
            patch("jobs.base_job.setup_logging") as mock_logging,
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
        ):
            mock_logger = MagicMock()
            mock_logging.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            job = SalesETLJob("test_job", {"env": "local", "JOB_RUN_ID": "test-123"})
            job.spark = spark

            # Transform to create aggregations
            job.transform(df)

            # Mock load method to use our temp directory
            def mock_load(main_df):
                daily_path = os.path.join(output_dir, "sales_daily_agg/")
                product_path = os.path.join(output_dir, "sales_product_agg/")
                customer_path = os.path.join(output_dir, "sales_customer_agg/")

                job.daily_agg.coalesce(1).write.mode("overwrite").option("header", "true").csv(daily_path)
                job.product_agg.coalesce(1).write.mode("overwrite").option("header", "true").csv(product_path)
                job.customer_agg.coalesce(1).write.mode("overwrite").option("header", "true").csv(customer_path)

                return [daily_path, product_path, customer_path]

            job.load = mock_load
            output_paths = job.load(None)  # main_df not needed for our mock

            # Verify all three output directories were created
            assert len(output_paths) == 3, "Expected 3 output paths"
            for path in output_paths:
                assert os.path.exists(path), f"Output directory not created: {path}"

                # Check that CSV files were written
                csv_files = list(Path(path).glob("*.csv"))
                assert len(csv_files) > 0, f"No CSV files written to {path}"
