"""
Cross-job integration tests that validate data dependencies and workflows
between different Glue jobs in the medallion architecture.
"""

import os
import tempfile
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.functions import col, lit
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

from jobs.customer_import import CustomerImportJob
from jobs.inventory_sync import InventorySyncJob
from jobs.sales_etl import SalesETLJob

from .test_job_runner import IntegrationTestRunner


class TestCrossJobIntegration:
    """Integration tests for cross-job data dependencies and workflows"""

    @pytest.fixture
    def runner(self):
        """Setup integration test runner"""
        runner = IntegrationTestRunner()
        runner.setup_mock_aws_services()
        yield runner
        runner.cleanup()

    @pytest.mark.integration
    def test_customer_to_sales_data_flow(self, runner):
        """Test that customer data properly flows to sales ETL processing"""
        spark = runner.setup_spark_session()
        temp_dir = runner.get_temp_dir()

        with (
            patch("jobs.base_job.setup_logging") as mock_logging,
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
        ):
            mock_logger = MagicMock()
            mock_logging.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            # Step 1: Run customer import job
            customer_job = CustomerImportJob("customer_import", {"env": "local", "JOB_RUN_ID": "test-customer-123"})
            customer_job.spark = spark

            # Mock customer data using builder
            def mock_customer_extract():
                from .test_data_builders import CustomerDataBuilder

                customer_data = (
                    CustomerDataBuilder()
                    .add_valid_customer("CUST001", "John", "Doe", "john.doe@example.com", "555-1234", "2023-01-15")
                    .add_valid_customer("CUST002", "Jane", "Smith", "jane.smith@example.com", "555-5678", "2023-01-16")
                    .build_tuples()
                )
                schema = CustomerDataBuilder().build_schema_columns()
                return spark.createDataFrame(customer_data, schema)

            customer_job.extract = mock_customer_extract

            # Mock customer load to write to temp location
            customer_output_path = os.path.join(temp_dir, "customers/")

            def mock_customer_load(df):
                df.coalesce(1).write.mode("overwrite").option("header", "true").csv(customer_output_path)
                return [customer_output_path]

            customer_job.load = mock_customer_load

            # Run customer job
            customer_df = customer_job.extract()
            customer_transformed = customer_job.transform(customer_df)
            customer_job.load(customer_transformed)

            # Verify customer data was written
            assert os.path.exists(customer_output_path), "Customer output directory not created"

            # Step 2: Run sales ETL job that depends on customer data
            sales_job = SalesETLJob("sales_etl", {"env": "local", "JOB_RUN_ID": "test-sales-123"})
            sales_job.spark = spark

            # Mock sales data that references the customers using builder
            def mock_sales_extract():
                from .test_data_builders import SalesDataBuilder

                sales_data = (
                    SalesDataBuilder()
                    .add_valid_transaction("TXN001", "CUST001", "PROD001", 2, 29.99, "2023-01-15")
                    .add_valid_transaction("TXN002", "CUST002", "PROD002", 1, 149.99, "2023-01-16")
                    .add_valid_transaction("TXN003", "CUST001", "PROD001", 1, 29.99, "2023-01-17")
                    .build_tuples()
                )
                sales_schema = SalesDataBuilder().build_schema_columns()
                df_sales = spark.createDataFrame(sales_data, sales_schema)

                # Products data
                products_data = [
                    ("PROD001", "Wireless Headphones", "Electronics", 15.00),
                    ("PROD002", "Smart Watch", "Electronics", 75.00),
                ]
                products_schema = "product_id string, product_name string, category string, cost double"
                df_products = spark.createDataFrame(products_data, products_schema)

                # Join with products (simulating what sales ETL does)
                return df_sales.join(df_products, on="product_id", how="left")

            sales_job.extract = mock_sales_extract

            # Mock sales load
            sales_output_dir = os.path.join(temp_dir, "sales/")

            def mock_sales_load(df):
                daily_path = os.path.join(sales_output_dir, "daily/")
                product_path = os.path.join(sales_output_dir, "product/")
                customer_path = os.path.join(sales_output_dir, "customer/")

                # Use the stored aggregations from transform
                sales_job.daily_agg.coalesce(1).write.mode("overwrite").option("header", "true").csv(daily_path)
                sales_job.product_agg.coalesce(1).write.mode("overwrite").option("header", "true").csv(product_path)
                sales_job.customer_agg.coalesce(1).write.mode("overwrite").option("header", "true").csv(customer_path)

                return [daily_path, product_path, customer_path]

            sales_job.load = mock_sales_load

            # Run sales job
            sales_df = sales_job.extract()
            sales_transformed = sales_job.transform(sales_df)
            sales_job.load(sales_transformed)

            # Step 3: Validate cross-job data consistency
            # Read customer aggregation from sales job
            customer_agg_results = sales_job.customer_agg.collect()

            # Should have customer aggregations for both CUST001 and CUST002
            customer_ids = {r["customer_id"] for r in customer_agg_results}
            assert customer_ids == {"CUST001", "CUST002"}, f"Expected customers CUST001, CUST002, got {customer_ids}"

            # Validate customer metrics (aggregated by customer per day)
            # CUST001 has transactions on 2 different days, so should have 2 records
            cust001_records = [r for r in customer_agg_results if r["customer_id"] == "CUST001"]
            assert len(cust001_records) == 2, f"CUST001 should have 2 daily records, got {len(cust001_records)}"

            # Each day should have 1 transaction for CUST001
            for record in cust001_records:
                assert record["transaction_count"] == 1, (
                    f"Each CUST001 daily record should have 1 transaction, got {record['transaction_count']}"
                )

            # Total revenue for CUST001 should be 89.97 (59.98 + 29.99)
            cust001_total_revenue = sum(r["customer_revenue"] for r in cust001_records)
            assert abs(cust001_total_revenue - 89.97) < 0.01, (
                f"CUST001 total revenue should be 89.97, got {cust001_total_revenue}"
            )

            # CUST002 has transaction on 1 day, so should have 1 record
            cust002_records = [r for r in customer_agg_results if r["customer_id"] == "CUST002"]
            assert len(cust002_records) == 1, f"CUST002 should have 1 daily record, got {len(cust002_records)}"
            assert cust002_records[0]["transaction_count"] == 1, (
                f"CUST002 should have 1 transaction, got {cust002_records[0]['transaction_count']}"
            )
            assert abs(cust002_records[0]["customer_revenue"] - 149.99) < 0.01, (
                f"CUST002 revenue should be 149.99, got {cust002_records[0]['customer_revenue']}"
            )

    def _setup_medallion_test_context(self, runner):
        """Helper method to setup common test context for medallion architecture tests."""
        spark = runner.setup_spark_session()

        mock_patches = [
            patch("jobs.base_job.setup_logging"),
            patch("jobs.base_job.AuditTracker"),
            patch("jobs.base_job.DLQHandler"),
            patch("jobs.base_job.NotificationService"),
        ]

        context = {}
        context["spark"] = spark
        context["patches"] = mock_patches

        return context

    def _create_bronze_customer_data(self, spark):
        """Create bronze layer customer data for medallion tests."""
        from .test_data_builders import CustomerDataBuilder

        # Create raw bronze data with mixed case emails and formatting issues
        customers = (
            CustomerDataBuilder()
            .add_valid_customer("CUST001", "John", "Doe", "JOHN.DOE@EXAMPLE.COM", "5551234567")
            .add_valid_customer("CUST002", "Jane", "Smith", "jane.smith@example.com", "(555) 987-6543", "2023-01-16")
            .build_tuples()
        )

        schema = "customer_id string, first_name string, last_name string, email string, phone string, registration_date string"
        return spark.createDataFrame(customers, schema)

    @pytest.mark.integration
    def test_bronze_to_silver_customer_transformation(self, runner):
        """Test bronze → silver customer data transformation with data cleansing."""
        context = self._setup_medallion_test_context(runner)
        spark = context["spark"]

        with ExitStack() as stack:
            # Apply all patches
            for patch_obj in context["patches"]:
                mock = stack.enter_context(patch_obj)
                if "logging" in str(patch_obj):
                    mock_logger = MagicMock()
                    mock.return_value = mock_logger
                    mock_logger.bind.return_value = mock_logger

            # Test bronze → silver transformation for customers
            customer_job = CustomerImportJob("customer_import", {"env": "local", "JOB_RUN_ID": "test-bronze-silver"})
            customer_job.spark = spark
            customer_job.extract = lambda: self._create_bronze_customer_data(spark)

            # Process bronze to silver
            customer_bronze = customer_job.extract()
            customer_silver = customer_job.transform(customer_bronze)
            customer_results = customer_silver.collect()

            # Validate silver layer transformations
            assert len(customer_results) == 2, "Should have 2 customers after silver processing"

            # Email normalization: JOHN.DOE@EXAMPLE.COM → john.doe@example.com
            customer_emails = {r["email"] for r in customer_results}
            assert "john.doe@example.com" in customer_emails, "Email should be normalized in silver layer"

            # Verify standardized processing metadata is added
            for customer in customer_results:
                assert customer["processed_timestamp"] is not None, "Processing timestamp should be added"
                assert customer["processed_by_job"] == "customer_import", "Job name should be added"

    @pytest.mark.integration
    def test_job_execution_order_validation(self, runner):
        """Test that jobs can be executed in proper dependency order"""
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

            # Define job execution order and dependencies
            execution_log = []

            def log_job_execution(job_name):
                execution_log.append(job_name)

            # Mock job executions to just log when they run
            jobs = {
                "customer_import": CustomerImportJob("customer_import", {"env": "local", "JOB_RUN_ID": "test-order-1"}),
                "inventory_sync": InventorySyncJob("inventory_sync", {"env": "local", "JOB_RUN_ID": "test-order-2"}),
                "sales_etl": SalesETLJob("sales_etl", {"env": "local", "JOB_RUN_ID": "test-order-3"}),
            }

            for job_name, job in jobs.items():
                job.spark = spark

                # Mock simple successful execution
                def make_extract(name):
                    def mock_extract():
                        log_job_execution(f"{name}_extract")
                        data = [("test_id", "test_value")]
                        schema = "id string, value string"
                        return spark.createDataFrame(data, schema)

                    return mock_extract

                def make_transform(name):
                    def mock_transform(df):
                        log_job_execution(f"{name}_transform")
                        return df.withColumn("processed", lit(True))

                    return mock_transform

                def make_validate(name):
                    def mock_validate(df):
                        log_job_execution(f"{name}_validate")
                        return True

                    return mock_validate

                def make_load(name):
                    def mock_load(df):
                        log_job_execution(f"{name}_load")
                        return [f"s3://test/{name}/"]

                    return mock_load

                job.extract = make_extract(job_name)
                job.transform = make_transform(job_name)
                job.validate = make_validate(job_name)
                job.load = make_load(job_name)

            # Execute jobs in dependency order
            # Level 1: Independent jobs (can run in parallel)
            jobs["customer_import"].run()

            # Level 2: Jobs that depend on Level 1
            jobs["inventory_sync"].run()  # Independent job for inventory management

            # Level 3: Jobs that depend on Level 1 & 2
            jobs["sales_etl"].run()  # Depends on customer data

            # Validate execution order
            assert len(execution_log) == 12, (
                f"Expected 12 execution steps (3 jobs × 4 phases), got {len(execution_log)}"
            )

            # Check that customer_import executed before sales_etl
            customer_extract_pos = execution_log.index("customer_import_extract")
            sales_extract_pos = execution_log.index("sales_etl_extract")

            assert customer_extract_pos < sales_extract_pos, "Customer import should execute before sales ETL"

            # Verify all jobs completed their full lifecycle
            for job_name in ["customer_import", "inventory_sync", "sales_etl"]:
                assert f"{job_name}_extract" in execution_log, f"{job_name} extract not executed"
                assert f"{job_name}_transform" in execution_log, f"{job_name} transform not executed"
                assert f"{job_name}_validate" in execution_log, f"{job_name} validate not executed"
                assert f"{job_name}_load" in execution_log, f"{job_name} load not executed"
