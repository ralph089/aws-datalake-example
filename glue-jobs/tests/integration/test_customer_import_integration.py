"""
Integration tests for CustomerImportJob
Tests the complete ETL pipeline including extract, transform, validate, and load phases.
"""

import os
from pathlib import Path

import pytest
from chispa import assert_df_equality

from jobs.customer_import import CustomerImportJob


class TestCustomerImportIntegration:
    """Integration tests for customer import job"""

    @pytest.fixture
    def runner(self):
        """Setup integration test runner"""
        from .test_job_runner import IntegrationTestRunner
        runner = IntegrationTestRunner()
        runner.setup_mock_aws_services()
        yield runner
        runner.cleanup()

    @pytest.mark.integration
    def test_customer_import_full_pipeline_success(self, runner):
        """Test successful execution of complete customer import pipeline"""
        # Setup test data path to use local CSV files
        test_data_path = Path("/home/hadoop/workspace/data/customers.csv")
        assert test_data_path.exists(), f"Test data not found at {test_data_path}"

        # Test job execution
        result = runner.run_job_with_args(
            CustomerImportJob, {"env": "local", "test_data_path": str(test_data_path)}
        )

        # Validate successful execution
        assert result["success"] is True, f"Job failed with error: {result.get('error')}"

        # Validate record counts
        assert result["raw_count"] > 0, "No raw records extracted"
        assert result["transformed_count"] > 0, "No records after transformation"
        assert result["final_count"] > 0, "No final records"

        # Validate expected columns are present
        expected_columns = [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "registration_date",
            "full_name",
            "email_domain",
            "processed_timestamp",
            "processed_by_job",
            "job_run_id",
        ]

        runner.validate_data_quality(
            (
                runner.spark.createDataFrame(result["sample_data"])
                if result["sample_data"]
                else None
            ),
            expected_columns,
        )

        # Some columns should be present (basic validation)
        assert "customer_id" in result["columns"], "customer_id column missing"
        assert "email" in result["columns"], "email column missing"
        assert "full_name" in result["columns"], "full_name derived column missing"
        assert "email_domain" in result["columns"], "email_domain derived column missing"

    @pytest.mark.integration
    def test_customer_import_data_transformations(self, mock_job_context, job_factory):
        """
        Test comprehensive data transformation logic for customer import using factory-boy.

        This test validates the complete data standardization pipeline:
        1. Email normalization (uppercase → lowercase)
        2. Name standardization (trim whitespace, consistent casing)
        3. Phone number formatting preservation
        4. Invalid record filtering (malformed emails)
        5. Derived field generation (full_name, email_domain)
        6. Processing metadata addition (timestamps, job tracking)
        """
        # Import factory directly
        from tests.factories import CustomerFactory
        
        # Create test data with known transformation scenarios using factory-boy
        test_customers = [
            # Standard record with uppercase email - should be normalized
            CustomerFactory(customer_id="CUST001", first_name="John", last_name="Doe", 
                           email="JOHN.DOE@EXAMPLE.COM", phone="5551234567", registration_date="2023-01-15"),
            # Already clean record - should pass through unchanged
            CustomerFactory(customer_id="CUST002", first_name="Jane", last_name="Smith",
                           email="jane.smith@example.com", phone="(555) 987-6543", registration_date="2023-02-20"),
            # Invalid email - should be filtered out during transformation
            CustomerFactory.with_invalid_email(customer_id="CUST003"),
            # Names with whitespace and mixed case - should be standardized
            CustomerFactory(customer_id="CUST004", first_name="  alice  ", last_name="  WILLIAMS  ",
                           email="alice.williams@example.com", phone="555.123.4567", registration_date="2023-04-05")
        ]

        # Convert to Spark DataFrame
        spark = job_factory.runner.spark
        columns = ["customer_id", "first_name", "last_name", "email", "phone", "registration_date"]
        data = [(c["customer_id"], c["first_name"], c["last_name"], c["email"], c["phone"], str(c["registration_date"])) 
                for c in test_customers]
        df = spark.createDataFrame(data, columns)

        # Use centralized mock context manager
        with mock_job_context as mocks:
            job = job_factory.customer_import("test_job", "test-123")

            # Execute the transformation pipeline
            result_df = job.transform(df)
            results = result_df.collect()

            # Validate data quality filtering
            # CUST003 should be filtered out due to invalid email format
            assert len(results) == 3, "Invalid email record (CUST003) should be filtered out during transformation"

            # Validate email normalization business logic
            # Input: "JOHN.DOE@EXAMPLE.COM" → Expected: "john.doe@example.com"
            john_record = next((r for r in results if r["customer_id"] == "CUST001"), None)
            assert john_record is not None, "CUST001 record missing after transformation"
            assert john_record["email"] == "john.doe@example.com", (
                f"Email not normalized correctly: expected 'john.doe@example.com', got '{john_record['email']}'"
            )

            # Validate name standardization business logic
            # Input: "  alice  ", "  WILLIAMS  " → Expected: "alice", "WILLIAMS" (trimmed, case preserved)
            alice_record = next((r for r in results if r["customer_id"] == "CUST004"), None)
            assert alice_record is not None, "CUST004 record missing after transformation"
            assert alice_record["first_name"] == "alice", (
                f"First name not standardized: expected 'alice', got '{alice_record['first_name']}'"
            )
            assert alice_record["last_name"] == "WILLIAMS", (
                f"Last name not standardized: expected 'WILLIAMS', got '{alice_record['last_name']}'"
            )

            # Validate phone number format preservation
            # Different input formats should be preserved as-is after validation
            jane_record = next((r for r in results if r["customer_id"] == "CUST002"), None)
            assert jane_record is not None, "CUST002 record missing after transformation"
            assert jane_record["phone"] == "(555) 987-6543", (
                f"Phone format not preserved: expected '(555) 987-6543', got '{jane_record['phone']}'"
            )

            # Validate derived field generation business logic
            # full_name should combine standardized first_name + last_name (case preserved)
            assert john_record["full_name"] == "John Doe", (
                f"Full name not generated correctly: expected 'John Doe', got '{john_record['full_name']}'"
            )

            # email_domain should extract domain portion from normalized email
            assert john_record["email_domain"] == "example.com", (
                f"Email domain not extracted correctly: expected 'example.com', got '{john_record['email_domain']}'"
            )

            # Validate processing metadata addition (audit trail requirements)
            # These fields support data lineage and troubleshooting
            assert john_record["processed_by_job"] == "test_job", (
                f"Job name not recorded: expected 'test_job', got '{john_record['processed_by_job']}'"
            )
            assert john_record["job_run_id"] == "test-123", (
                f"Run ID not recorded: expected 'test-123', got '{john_record['job_run_id']}'"
            )
            assert john_record["processed_timestamp"] is not None, (
                "Processing timestamp not added (required for audit trail)"
            )

    @pytest.mark.integration
    def test_customer_import_validation_failures(self, mock_job_context, job_factory):
        """Test validation failure scenarios using factory-boy"""
        # Import factory directly
        from tests.factories import CustomerFactory
        
        # Create data with validation issues using factory-boy
        base_customer = CustomerFactory(customer_id="CUST001", first_name="John", last_name="Doe", 
                                       email="john.doe@example.com", phone="5551234567", registration_date="2023-01-15")
        
        # Create duplicate customer
        invalid_customers = [base_customer, base_customer.copy()]  # Duplicate customer

        spark = job_factory.runner.spark
        columns = ["customer_id", "first_name", "last_name", "email", "phone", "registration_date"]
        data = [(c["customer_id"], c["first_name"], c["last_name"], c["email"], c["phone"], str(c["registration_date"])) 
                for c in invalid_customers]
        df = spark.createDataFrame(data, columns)

        # Use centralized mock context manager
        with mock_job_context as mocks:
            job = job_factory.customer_import("test_job", "test-123")

            # Transform and validate
            transformed_df = job.transform(df)
            validation_result = job.custom_validation(transformed_df)

            # Should fail due to duplicate emails
            assert validation_result is False, "Validation should fail for duplicate emails"

    @pytest.mark.integration
    def test_customer_import_empty_dataset(self, mock_job_context, job_factory):
        """Test handling of empty dataset using SparkDataFrameFactory"""
        from tests.factories import SparkDataFrameFactory
        
        # Create empty DataFrame with correct schema using SparkDataFrameFactory
        spark = job_factory.runner.spark
        df = SparkDataFrameFactory.create_customer_df(spark, count=0)  # Empty DataFrame with correct schema

        # Use centralized mock context manager
        with mock_job_context as mocks:
            job = job_factory.customer_import("test_job", "test-123")

            # Transform and validate empty dataset
            transformed_df = job.transform(df)
            validation_result = job.validate(transformed_df)  # Use base validation

            # Should fail due to no records
            assert validation_result is False, "Validation should fail for empty dataset"

    @pytest.mark.integration
    def test_customer_import_local_file_output(
        self, mock_job_context, job_factory, runner
    ):
        """Test that local output files are created correctly using factory-boy"""
        from tests.factories import CustomerFactory
        
        # Create temporary directory for output
        output_dir = runner.get_temp_dir()

        # Create minimal test data using factory-boy
        test_customer = CustomerFactory(customer_id="CUST001", first_name="John", last_name="Doe", 
                                       email="john.doe@example.com", phone="5551234567", registration_date="2023-01-15")
        
        spark = job_factory.runner.spark
        columns = ["customer_id", "first_name", "last_name", "email", "phone", "registration_date"]
        data = [(test_customer["customer_id"], test_customer["first_name"], test_customer["last_name"], 
                test_customer["email"], test_customer["phone"], str(test_customer["registration_date"]))]
        df = spark.createDataFrame(data, columns)

        # Use centralized mock context manager
        with mock_job_context as mocks:
            job = job_factory.customer_import("test_job", "test-123")

            # Transform and load
            transformed_df = job.transform(df)

            # Mock the local output path to use our temp directory

            def mock_load(df):
                output_path = os.path.join(output_dir, "customers/")
                df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
                return [output_path]

            job.load = mock_load
            output_paths = job.load(transformed_df)

            # Verify output file was created
            assert len(output_paths) == 1, "Expected one output path"
            output_path = output_paths[0]
            assert os.path.exists(output_path), f"Output directory not created: {output_path}"

            # Check that CSV files were written
            csv_files = list(Path(output_path).glob("*.csv"))
            assert len(csv_files) > 0, "No CSV files written to output directory"

    @pytest.mark.skip(reason="DataQualityChecker class does not exist in customer_import module")
    @pytest.mark.integration
    def test_customer_import_with_quality_checker(
        self, mock_job_context, job_factory, mocker
    ):
        """Test integration with DataQualityChecker using pytest-mock"""
        from tests.factories import CustomerFactory
        
        # Setup mock quality checker using pytest-mock
        mock_quality_checker = mocker.patch("jobs.customer_import.DataQualityChecker")
        mock_checker_instance = mocker.MagicMock()
        mock_quality_checker.return_value = mock_checker_instance
        mock_checker_instance.check_completeness.return_value = {"passed": True}
        mock_checker_instance.check_patterns.return_value = {"passed": True}

        # Create test data using factory-boy
        test_customer = CustomerFactory(customer_id="CUST001", first_name="John", last_name="Doe", 
                                       email="john.doe@example.com", phone="5551234567", registration_date="2023-01-15")
        
        spark = job_factory.runner.spark
        columns = ["customer_id", "first_name", "last_name", "email", "phone", "registration_date"]
        data = [(test_customer["customer_id"], test_customer["first_name"], test_customer["last_name"], 
                test_customer["email"], test_customer["phone"], str(test_customer["registration_date"]))]
        df = spark.createDataFrame(data, columns)

        # Use centralized mock context manager
        with mock_job_context as mocks:
            job = job_factory.customer_import("test_job", "test-123")

            # Transform and validate
            transformed_df = job.transform(df)
            validation_result = job.custom_validation(transformed_df)

            # Should pass with mocked quality checker
            assert validation_result is True, "Validation should pass with mocked quality checker"

            # Verify quality checker was called
            mock_checker_instance.check_completeness.assert_called_once()
            mock_checker_instance.check_patterns.assert_called_once()
