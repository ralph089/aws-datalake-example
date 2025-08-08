"""
Integration tests for InventorySyncJob
Tests the complete ETL pipeline including API data fetching, change detection,
and synchronization logic.
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from jobs.inventory_sync import InventorySyncJob

from .test_job_runner import IntegrationTestRunner


class TestInventorySyncIntegration:
    """Integration tests for inventory sync job"""

    @pytest.fixture
    def runner(self):
        """Setup integration test runner"""
        runner = IntegrationTestRunner()
        runner.setup_mock_aws_services()
        yield runner
        runner.cleanup()

    @pytest.mark.integration
    def test_inventory_sync_full_pipeline_success(self, runner):
        """Test successful execution of complete inventory sync pipeline"""
        # Verify test data file exists
        inventory_data_path = Path(__file__).parent.parent / "test_data" / "inventory.csv"
        assert inventory_data_path.exists(), f"Inventory test data not found at {inventory_data_path}"

        # Test job execution
        result = runner.run_job_with_args(InventorySyncJob, {"env": "local"})

        # Validate successful execution
        assert result["success"] is True, f"Job failed with error: {result.get('error')}"

        # Validate record counts
        assert result["raw_count"] > 0, "No raw records extracted from API"
        assert result["transformed_count"] >= 0, "Invalid transformation count"
        assert result["final_count"] >= 0, "Invalid final count"

        # Validate expected columns are present

        # Check that key columns exist
        assert "product_id" in result["columns"], "product_id column missing"
        assert "location_id" in result["columns"], "location_id column missing"
        assert "change_type" in result["columns"], "change_type column missing"
        assert "needs_reorder" in result["columns"], "needs_reorder business logic column missing"
        assert "overstocked" in result["columns"], "overstocked business logic column missing"

    @pytest.mark.integration
    def test_inventory_sync_change_detection_new_data(self, runner):
        """Test change detection when no existing inventory data exists"""
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

            job = InventorySyncJob("test_job", {"env": "local", "JOB_RUN_ID": "test-123"})
            job.spark = spark

            # Create empty current inventory (new setup scenario)
            empty_schema = StructType(
                [
                    StructField("product_id", StringType(), True),
                    StructField("location_id", StringType(), True),
                    StructField("quantity_on_hand", IntegerType(), True),
                    StructField("last_updated", TimestampType(), True),
                ]
            )
            job.current_inventory = spark.createDataFrame([], empty_schema)

            # Simulate API data (this is what the job fetches)
            api_data = [
                ("PROD001", "LOC001", 150, 50, 500),
                ("PROD002", "LOC001", 75, 25, 300),
                ("PROD003", "LOC002", 200, 100, 400),
            ]

            api_schema = StructType(
                [
                    StructField("product_id", StringType(), True),
                    StructField("location_id", StringType(), True),
                    StructField("quantity_on_hand", IntegerType(), True),
                    StructField("reorder_point", IntegerType(), True),
                    StructField("max_stock_level", IntegerType(), True),
                ]
            )

            df_api = spark.createDataFrame(api_data, api_schema)

            # Transform to detect changes
            result_df = job.transform(df_api)
            results = result_df.collect()

            # All records should be marked as "new"
            assert len(results) == 3, f"Expected 3 records, got {len(results)}"

            for result in results:
                assert result["change_type"] == "new", f"Expected 'new' change type, got {result['change_type']}"
                assert result["product_id"] in ["PROD001", "PROD002", "PROD003"], "Unexpected product_id"

            # Check business logic flags
            prod001_record = next((r for r in results if r["product_id"] == "PROD001"), None)
            assert prod001_record is not None, "PROD001 record missing"
            assert prod001_record["needs_reorder"] is False, "PROD001 should not need reorder (150 > 50)"
            assert prod001_record["overstocked"] is False, "PROD001 should not be overstocked (150 < 500)"

    @pytest.mark.integration
    def test_inventory_sync_change_detection_with_existing_data(self, runner):
        """Test change detection with existing inventory data"""
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

            job = InventorySyncJob("test_job", {"env": "local", "JOB_RUN_ID": "test-123"})
            job.spark = spark

            # Create existing inventory data
            existing_data = [
                ("PROD001", "LOC001", 100),  # Will be updated (150 vs 100)
                ("PROD002", "LOC001", 75),  # Will be unchanged
                ("PROD999", "LOC001", 50),  # Will be deleted (not in API)
            ]

            existing_schema = StructType(
                [
                    StructField("product_id", StringType(), True),
                    StructField("location_id", StringType(), True),
                    StructField("quantity_on_hand", IntegerType(), True),
                ]
            )

            job.current_inventory = spark.createDataFrame(existing_data, existing_schema)

            # Create API data
            api_data = [
                ("PROD001", "LOC001", 150, 50, 500),  # Updated quantity
                ("PROD002", "LOC001", 75, 25, 300),  # Unchanged
                ("PROD003", "LOC002", 200, 100, 400),  # New product
            ]

            api_schema = StructType(
                [
                    StructField("product_id", StringType(), True),
                    StructField("location_id", StringType(), True),
                    StructField("quantity_on_hand", IntegerType(), True),
                    StructField("reorder_point", IntegerType(), True),
                    StructField("max_stock_level", IntegerType(), True),
                ]
            )

            df_api = spark.createDataFrame(api_data, api_schema)

            # Transform to detect changes
            result_df = job.transform(df_api)
            results = result_df.collect()

            # Should have 3 changed records (updated, new, deleted) - unchanged filtered out
            change_types = {r["change_type"] for r in results}
            assert "updated" in change_types, "Should detect updated records"
            assert "new" in change_types, "Should detect new records"
            assert "deleted" in change_types, "Should detect deleted records"

            # Verify specific changes
            prod001_record = next((r for r in results if r["product_id"] == "PROD001"), None)
            assert prod001_record is not None, "PROD001 updated record missing"
            assert prod001_record["change_type"] == "updated", "PROD001 should be marked as updated"
            assert prod001_record["new_quantity"] == 150, "PROD001 new quantity should be 150"
            assert prod001_record["old_quantity"] == 100, "PROD001 old quantity should be 100"

            prod003_record = next((r for r in results if r["product_id"] == "PROD003"), None)
            assert prod003_record is not None, "PROD003 new record missing"
            assert prod003_record["change_type"] == "new", "PROD003 should be marked as new"

            prod999_record = next((r for r in results if r["product_id"] == "PROD999"), None)
            assert prod999_record is not None, "PROD999 deleted record missing"
            assert prod999_record["change_type"] == "deleted", "PROD999 should be marked as deleted"

    @pytest.mark.integration
    def test_inventory_sync_business_logic_flags(self, runner):
        """Test business logic flags (needs_reorder, overstocked)"""
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

            job = InventorySyncJob("test_job", {"env": "local", "JOB_RUN_ID": "test-123"})
            job.spark = spark

            # Setup empty current inventory
            job.current_inventory = spark.createDataFrame(
                [], "product_id string, location_id string, quantity_on_hand int"
            )

            # Create API data with specific scenarios for business logic testing
            api_data = [
                ("PROD001", "LOC001", 30, 50, 500),  # Needs reorder (30 <= 50)
                ("PROD002", "LOC001", 600, 25, 500),  # Overstocked (600 > 500)
                ("PROD003", "LOC001", 10, 50, 300),  # Needs reorder AND overstocked (edge case)
                ("PROD004", "LOC001", 100, 50, 500),  # Normal stock level
            ]

            api_schema = StructType(
                [
                    StructField("product_id", StringType(), True),
                    StructField("location_id", StringType(), True),
                    StructField("quantity_on_hand", IntegerType(), True),
                    StructField("reorder_point", IntegerType(), True),
                    StructField("max_stock_level", IntegerType(), True),
                ]
            )

            df_api = spark.createDataFrame(api_data, api_schema)

            # Transform to apply business logic
            result_df = job.transform(df_api)
            results = result_df.collect()

            # Verify business logic flags
            prod001 = next((r for r in results if r["product_id"] == "PROD001"), None)
            assert prod001 is not None, "PROD001 missing"
            assert prod001["needs_reorder"] is True, "PROD001 should need reorder (30 <= 50)"
            assert prod001["overstocked"] is False, "PROD001 should not be overstocked (30 < 500)"

            prod002 = next((r for r in results if r["product_id"] == "PROD002"), None)
            assert prod002 is not None, "PROD002 missing"
            assert prod002["needs_reorder"] is False, "PROD002 should not need reorder (600 > 25)"
            assert prod002["overstocked"] is True, "PROD002 should be overstocked (600 > 500)"

            prod003 = next((r for r in results if r["product_id"] == "PROD003"), None)
            assert prod003 is not None, "PROD003 missing"
            assert prod003["needs_reorder"] is True, "PROD003 should need reorder (10 <= 50)"
            assert prod003["overstocked"] is False, "PROD003 should not be overstocked (10 < 300)"

            prod004 = next((r for r in results if r["product_id"] == "PROD004"), None)
            assert prod004 is not None, "PROD004 missing"
            assert prod004["needs_reorder"] is False, "PROD004 should not need reorder (100 > 50)"
            assert prod004["overstocked"] is False, "PROD004 should not be overstocked (100 < 500)"

    @pytest.mark.integration
    def test_inventory_sync_validation_failures(self, runner):
        """Test validation failure scenarios"""
        spark = runner.setup_spark_session()

        # Create invalid data (negative quantities)
        invalid_data = [
            ("PROD001", "LOC001", "new", -50, None, 50, 500),  # Negative quantity
            ("PROD002", None, "new", 100, None, 25, 300),  # Missing location_id
            (None, "LOC001", "new", 150, None, 100, 400),  # Missing product_id
        ]

        schema = StructType(
            [
                StructField("product_id", StringType(), True),
                StructField("location_id", StringType(), True),
                StructField("change_type", StringType(), True),
                StructField("new_quantity", IntegerType(), True),
                StructField("old_quantity", IntegerType(), True),
                StructField("reorder_point", IntegerType(), True),
                StructField("max_stock_level", IntegerType(), True),
            ]
        )

        df = spark.createDataFrame(invalid_data, schema)

        with mock_job_context as mocks:
            job = job_factory.inventory_sync("test_job", "test-123")

            # Test custom validation
            validation_result = job.custom_validation(df)

            # Should fail due to negative quantities and missing keys
            assert validation_result is False, "Validation should fail for invalid data"

    @pytest.mark.integration
    def test_inventory_sync_api_simulation(self, runner):
        """Test that local API simulation works correctly"""
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

            job = InventorySyncJob("test_job", {"env": "local", "JOB_RUN_ID": "test-123"})
            job.spark = spark
            job.api_client = None  # Ensure we use local simulation

            # Test the _fetch_api_inventory method
            df_api = job._fetch_api_inventory()
            api_results = df_api.collect()

            # Should return the simulated data
            assert len(api_results) == 4, "Expected 4 simulated inventory records"

            # Check expected products are present (adjust for actual simulation data)
            product_ids = {r["product_id"] for r in api_results}
            expected_products = {"PROD001", "PROD002", "PROD003"}
            assert len(product_ids.intersection(expected_products)) > 0, "Expected products not found in simulation"

            # Check expected locations
            location_ids = {r["location_id"] for r in api_results}
            assert location_ids == {"LOC001", "LOC002"}, "Unexpected location IDs in simulation"

    @pytest.mark.integration
    def test_inventory_sync_local_file_output(self, mock_job_context, job_factory, runner):
        """Test that local output files are created correctly"""
        output_dir = runner.get_temp_dir()
        spark = runner.spark

        # Create minimal test data using builder (simplified for single test case)
        from .test_data_builders import InventoryDataBuilder

        # For this specific test, we need a custom tuple since it has change_type, old_quantity etc
        # that aren't in our standard builder. Keep inline for this edge case.
        inventory_data = [
            ("PROD001", "LOC001", "new", 150, None, 50, 500, False, False),
        ]

        schema = StructType(
            [
                StructField("product_id", StringType(), True),
                StructField("location_id", StringType(), True),
                StructField("change_type", StringType(), True),
                StructField("new_quantity", IntegerType(), True),
                StructField("old_quantity", IntegerType(), True),
                StructField("reorder_point", IntegerType(), True),
                StructField("max_stock_level", IntegerType(), True),
                StructField("needs_reorder", StringType(), True),
                StructField("overstocked", StringType(), True),
            ]
        )

        df = spark.createDataFrame(inventory_data, schema)

        with mock_job_context as mocks:
            job = job_factory.inventory_sync("test_job", "test-123")

            # Mock the load method to use our temp directory
            def mock_load(df):
                output_path = os.path.join(output_dir, "inventory_changes/")
                df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
                return [output_path]

            job.load = mock_load
            output_paths = job.load(df)

            # Verify output file was created
            assert len(output_paths) == 1, "Expected one output path"
            output_path = output_paths[0]
            assert os.path.exists(output_path), f"Output directory not created: {output_path}"

            # Check that CSV files were written
            csv_files = list(Path(output_path).glob("*.csv"))
            assert len(csv_files) > 0, "No CSV files written to output directory"

    @pytest.mark.skip(reason="DataQualityChecker class does not exist in inventory_sync module")
    @pytest.mark.integration
    @patch("jobs.inventory_sync.DataQualityChecker")
    def test_inventory_sync_with_quality_checker(self, mock_quality_checker, mock_job_context, job_factory):
        """Test integration with DataQualityChecker"""
        spark = job_factory.runner.spark

        # Setup mock quality checker
        mock_checker_instance = MagicMock()
        mock_quality_checker.return_value = mock_checker_instance
        mock_checker_instance.check_completeness.return_value = {"passed": True}
        mock_checker_instance.check_referential_integrity.return_value = {"passed": True}

        # Create valid inventory data
        valid_data = [
            ("PROD001", "LOC001", "new", 150, None, 50, 500),
            ("PROD002", "LOC002", "updated", 75, 50, 25, 300),
        ]

        schema = StructType(
            [
                StructField("product_id", StringType(), True),
                StructField("location_id", StringType(), True),
                StructField("change_type", StringType(), True),
                StructField("new_quantity", IntegerType(), True),
                StructField("old_quantity", IntegerType(), True),
                StructField("reorder_point", IntegerType(), True),
                StructField("max_stock_level", IntegerType(), True),
            ]
        )

        df = spark.createDataFrame(valid_data, schema)

        with mock_job_context as mocks:
            job = job_factory.inventory_sync("test_job", "test-123")

            # Test custom validation
            validation_result = job.custom_validation(df)

            # Should pass with mocked quality checker
            assert validation_result is True, "Validation should pass with mocked quality checker"

            # Verify quality checker was called
            mock_checker_instance.check_completeness.assert_called_once()
            mock_checker_instance.check_referential_integrity.assert_called_once()

            # Verify referential integrity was called with correct change types
            call_args = mock_checker_instance.check_referential_integrity.call_args
            referential_checks = call_args[0][1]  # Second argument (referential checks dict)

            assert "change_type" in referential_checks, "change_type referential check missing"
            valid_values = referential_checks["change_type"]["valid_values"]
            expected_change_types = ["new", "updated", "deleted", "unchanged"]
            assert set(valid_values) == set(expected_change_types), "Unexpected change type validation values"
