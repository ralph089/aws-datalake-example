"""
Integration tests for error handling, DLQ functionality, and notification systems.
Tests the BaseGlueJob error handling lifecycle and supporting infrastructure.
"""

import pytest
from unittest.mock import call
from pyspark.sql.functions import lit

from jobs.customer_import import CustomerImportJob
from jobs.sales_etl import SalesETLJob


class TestErrorHandlingIntegration:
    """Integration tests for error handling and DLQ functionality"""

    @pytest.mark.integration
    def test_job_failure_triggers_dlq_and_notifications(self, mock_failure_context, job_factory):
        """Test that job failures properly trigger DLQ messages and failure notifications"""
        from tests.factories import SparkDataFrameFactory
        
        with mock_failure_context as mocks:
            # Create a job that will fail during transform
            job = job_factory.customer_import("failing_job", "test-failure-123")
            spark = job.spark

            # Mock extract to return valid data using factory-boy
            def mock_extract():
                # Create minimal valid data for extract phase using factory-boy
                return SparkDataFrameFactory.create_customer_df(spark, count=1)

            job.extract = mock_extract

            # Mock transform to raise an exception
            def failing_transform(df):
                raise ValueError("Simulated transform failure for testing")

            job.transform = failing_transform

            # Run the job and expect it to fail
            with pytest.raises(ValueError, match="Simulated transform failure"):
                job.run()

            # Verify audit tracker was called
            mocks["audit"].start_job.assert_called_once()
            mocks["audit"].complete_job.assert_called_once_with("FAILED", "Simulated transform failure for testing")

            # Verify DLQ handler was called
            mocks["dlq"].send_to_dlq.assert_called_once()
            dlq_call_args = mocks["dlq"].send_to_dlq.call_args[0][0]
            assert dlq_call_args["job_name"] == "failing_job"
            assert dlq_call_args["job_run_id"] == "test-failure-123"
            assert "Simulated transform failure" in dlq_call_args["error"]
            assert "timestamp" in dlq_call_args

            # Verify failure notification was sent
            mocks["notifications"].send_failure_notification.assert_called_once()
            failure_call_args = mocks["notifications"].send_failure_notification.call_args[0][0]
            assert failure_call_args["job_name"] == "failing_job"
            assert failure_call_args["job_run_id"] == "test-failure-123"
            assert "Simulated transform failure" in failure_call_args["error"]

    @pytest.mark.integration
    def test_job_success_triggers_success_notification(self, mock_job_context, job_factory):
        """Test that successful jobs trigger success notifications and complete audit trail"""
        from tests.factories import SparkDataFrameFactory
        
        with mock_job_context as mocks:
            # Create a job with simple successful implementation
            job = job_factory.customer_import("successful_job", "test-success-123")
            spark = job.spark

            # Mock all phases to be successful using factory-boy
            def mock_extract():
                return SparkDataFrameFactory.create_customer_df(spark, count=1)

            def mock_transform(df):
                # Simple transformation - just add a column
                return df.withColumn("processed", lit(True))

            def mock_validate(df):
                return True  # Always pass validation

            def mock_load(df):
                return ["s3://test-bucket/output/"]

            job.extract = mock_extract
            job.transform = mock_transform
            job.validate = mock_validate
            job.load = mock_load

            # Run the job successfully
            job.run()

            # Verify audit tracker was called correctly
            mocks["audit"].start_job.assert_called_once()
            mocks["audit"].log_extract.assert_called_once_with(1)  # 1 record extracted
            mocks["audit"].log_transform.assert_called_once_with(1)  # 1 record transformed
            mocks["audit"].log_load.assert_called_once_with(["s3://test-bucket/output/"])
            mocks["audit"].complete_job.assert_called_once_with("SUCCESS")

            # Verify DLQ handler was NOT called
            mocks["dlq"].send_to_dlq.assert_not_called()

            # Verify success notification was sent
            mocks["notifications"].send_success_notification.assert_called_once()
            success_call_args = mocks["notifications"].send_success_notification.call_args[0][0]
            assert success_call_args["job_name"] == "successful_job"
            assert success_call_args["job_run_id"] == "test-success-123"
            assert success_call_args["records_processed"] == 1
            assert success_call_args["output_paths"] == ["s3://test-bucket/output/"]

    @pytest.mark.integration
    def test_validation_failure_triggers_error_handling(self, mock_failure_context, job_factory):
        """Test that validation failures are properly handled"""
        from tests.factories import SparkDataFrameFactory
        
        with mock_failure_context as mocks:
            # Create a job that will fail validation
            job = job_factory.customer_import("validation_failing_job", "test-validation-123")
            spark = job.spark

            # Mock phases using factory-boy
            def mock_extract():
                return SparkDataFrameFactory.create_customer_df(spark, count=1)

            def mock_transform(df):
                return df.withColumn("processed", lit(True))

            def failing_validate(df):
                return False  # Always fail validation

            job.extract = mock_extract
            job.transform = mock_transform
            job.validate = failing_validate

            # Run the job and expect validation failure
            with pytest.raises(ValueError, match="Data validation failed"):
                job.run()

            # Verify appropriate audit and error handling occurred
            mocks["audit"].complete_job.assert_called_once_with("FAILED", "Data validation failed")
            mocks["dlq"].send_to_dlq.assert_called_once()
            mocks["notifications"].send_failure_notification.assert_called_once()

    @pytest.mark.integration
    def test_extract_phase_failure_handling(self, mock_failure_context, job_factory):
        """Test error handling when extract phase fails"""
        with mock_failure_context as mocks:
            job = job_factory.customer_import("extract_failing_job", "test-extract-123")

            # Mock extract to fail
            def failing_extract():
                raise FileNotFoundError("Test data file not found")

            job.extract = failing_extract

            # Run the job and expect extract failure
            with pytest.raises(FileNotFoundError, match="Test data file not found"):
                job.run()

            # Verify error handling
            mocks["audit"].start_job.assert_called_once()
            mocks["audit"].complete_job.assert_called_once_with("FAILED", "Test data file not found")
            mocks["dlq"].send_to_dlq.assert_called_once()
            mocks["notifications"].send_failure_notification.assert_called_once()

    @pytest.mark.integration
    def test_load_phase_failure_handling(self, mock_failure_context, job_factory):
        """Test error handling when load phase fails"""
        from tests.factories import SparkDataFrameFactory
        
        with mock_failure_context as mocks:
            job = job_factory.customer_import("load_failing_job", "test-load-123")
            spark = job.spark

            # Mock successful phases except load using factory-boy
            def mock_extract():
                return SparkDataFrameFactory.create_customer_df(spark, count=1)

            def mock_transform(df):
                return df.withColumn("processed", lit(True))

            def mock_validate(df):
                return True

            def failing_load(df):
                raise PermissionError("Access denied to output location")

            job.extract = mock_extract
            job.transform = mock_transform
            job.validate = mock_validate
            job.load = failing_load

            # Run the job and expect load failure
            with pytest.raises(PermissionError, match="Access denied to output location"):
                job.run()

            # Verify error handling
            mocks["audit"].complete_job.assert_called_once_with("FAILED", "Access denied to output location")
            mocks["dlq"].send_to_dlq.assert_called_once()
            mocks["notifications"].send_failure_notification.assert_called_once()


    @pytest.mark.integration
    def test_transform_phase_failure_handling(self, mock_failure_context, job_factory):
        """Test error handling during transform phase with tracing"""
        from tests.factories import SparkDataFrameFactory
        
        with mock_failure_context as mocks:
            job = job_factory.customer_import("transform_failing_job", "test-transform-456")
            spark = job.spark

            # Mock extract to succeed, transform to fail
            def mock_extract():
                return SparkDataFrameFactory.create_customer_df(spark, count=1)

            def failing_transform(df):
                raise RuntimeError("Transform error for testing")

            job.extract = mock_extract
            job.transform = failing_transform

            # Run the job and expect failure
            with pytest.raises(RuntimeError, match="Transform error for testing"):
                job.run()

            expected_calls = [
                call("extract_phase"),
                call("transform_phase"),  # This phase will fail
            ]

            # Should have been called for extract and transform phases

