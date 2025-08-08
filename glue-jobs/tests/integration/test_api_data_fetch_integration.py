"""
Integration tests for APIDataFetchJob
Tests the complete ETL pipeline including API extraction, transformation, validation, and loading.
"""

import json
import os
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from jobs.api_data_fetch import APIDataFetchJob


class TestAPIDataFetchIntegration:
    """Integration tests for API data fetch job"""

    @pytest.fixture
    def runner(self):
        """Setup integration test runner"""
        from .test_job_runner import IntegrationTestRunner
        runner = IntegrationTestRunner()
        runner.setup_mock_aws_services()
        yield runner
        runner.cleanup()

    @pytest.fixture
    def mock_api_responses(self):
        """Mock API responses for testing"""
        # Page 1 response
        page_1_response = {
            "data": [
                {
                    "id": "api_001",
                    "name": "Product Alpha",
                    "value": "150.00",
                    "created_at": "2023-01-15T10:30:00Z",
                    "category": "electronics"
                },
                {
                    "id": "api_002",
                    "name": "Product Beta",
                    "value": "75.50",
                    "created_at": "2023-01-16T14:45:00Z",
                    "category": "books"
                }
            ],
            "pagination": {
                "page": 1,
                "limit": 2,
                "total_pages": 2,
                "total_count": 3
            }
        }

        # Page 2 response
        page_2_response = {
            "data": [
                {
                    "id": "api_003",
                    "name": "Product Gamma",
                    "value": "1200.00",
                    "created_at": "2023-01-17T09:15:00Z",
                    "category": "electronics"
                }
            ],
            "pagination": {
                "page": 2,
                "limit": 2,
                "total_pages": 2,
                "total_count": 3
            }
        }

        return [page_1_response, page_2_response]

    def test_api_data_fetch_full_pipeline_success(self, runner):
        """Test successful execution of complete API data fetch pipeline"""
        # Setup test data path to use local JSON file
        test_data_path = Path("/home/hadoop/workspace/data/api_data_fetch/api_response.json")
        if not test_data_path.exists():
            # Use relative path for local testing
            test_data_path = Path("tests/test_data/api_data_fetch/api_response.json")

        assert test_data_path.exists(), f"Test data not found at {test_data_path}"

        # Test job execution
        result = runner.run_job_with_args(
            APIDataFetchJob, {
                "env": "local",
                "test_data_path": str(test_data_path),
                "api_endpoint": "/api/v1/products",
                "page_size": "100",
                "max_pages": "5"
            }
        )

        # Validate successful execution
        assert result["success"] is True, f"Job failed with error: {result.get('error')}"

        # Validate record counts
        assert result["raw_count"] > 0, "No raw records extracted"
        assert result["transformed_count"] > 0, "No records after transformation"
        assert result["final_count"] > 0, "No final records"

        # Validate expected columns are present
        expected_columns = [
            "id",
            "name",
            "value",
            "created_at",
            "category",
            "metadata_source",
            "metadata_priority",
            "has_valid_id",
            "api_endpoint",
            "extraction_timestamp",
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

        # Validate specific columns are present
        assert "id" in result["columns"], "id column missing"
        assert "name" in result["columns"], "name column missing"
        assert "has_valid_id" in result["columns"], "has_valid_id derived column missing"
        assert "api_endpoint" in result["columns"], "api_endpoint metadata column missing"

    @pytest.mark.integration
    def test_api_data_fetch_with_mocked_api(self, mock_job_context, mock_api_responses, job_factory):
        """Test API data fetch with mocked API responses"""

        # Create mock API client
        mock_api_client = Mock()
        mock_api_client.get.side_effect = mock_api_responses

        # Use centralized mock context manager
        with mock_job_context as mocks:
            job = job_factory.create_job(
                APIDataFetchJob,
                "api_data_fetch",
                "test-run-123",
                {
                    "env": "dev",
                    "api_endpoint": "/api/v1/products",
                    "page_size": "2",
                    "max_pages": "2"
                }
            )

            # Override the API client with our mock
            job.api_client = mock_api_client

            # Execute the job pipeline
            raw_df = job.extract()
            assert raw_df.count() == 3, "Should extract 3 records from 2 API pages"

            transformed_df = job.transform(raw_df)
            assert transformed_df.count() == 3, "All records should be transformed"

            # Validate transformation results
            results = transformed_df.collect()

            # Check that API metadata was added
            for row in results:
                assert row["api_endpoint"] == "/api/v1/products"
                assert row["has_valid_id"] is True  # All test records have valid IDs
                assert row["processed_by_job"] == "api_data_fetch"
                assert row["job_run_id"] == "test-run-123"

            # Validate the data
            validation_result = job.validate(transformed_df)
            assert validation_result is True, "Validation should pass for mocked API data"

            # Verify API was called correctly
            assert mock_api_client.get.call_count == 2, "Should call API twice for 2 pages"

            # Verify pagination parameters
            first_call_params = mock_api_client.get.call_args_list[0][1]["params"]
            assert first_call_params["page"] == 1
            assert first_call_params["limit"] == 2

            second_call_params = mock_api_client.get.call_args_list[1][1]["params"]
            assert second_call_params["page"] == 2
            assert second_call_params["limit"] == 2

    @pytest.mark.integration
    def test_api_data_fetch_json_flattening(self, mock_job_context, job_factory):
        """Test JSON structure flattening functionality"""

        # Create test data with nested structures
        nested_api_response = {
            "data": [
                {
                    "id": "nested_001",
                    "name": "Nested Product",
                    "value": "99.99",
                    "metadata": {
                        "source": "external_api",
                        "priority": "high",
                        "tags": ["electronics", "featured"]
                    },
                    "created_at": "2023-01-15T10:30:00Z"
                }
            ],
            "pagination": {"page": 1, "total_pages": 1}
        }

        mock_api_client = Mock()
        mock_api_client.get.return_value = nested_api_response

        with mock_job_context as mocks:
            job = job_factory.create_job(
                APIDataFetchJob,
                "api_data_fetch",
                "test-run-123",
                {"env": "dev", "api_endpoint": "/api/v1/nested"}
            )

            job.api_client = mock_api_client

            # Execute extraction and transformation
            raw_df = job.extract()
            transformed_df = job.transform(raw_df)

            # Verify flattening occurred
            columns = transformed_df.columns
            assert "metadata_source" in columns, "Nested metadata.source should be flattened"
            assert "metadata_priority" in columns, "Nested metadata.priority should be flattened"

            # Verify original nested column was removed
            assert "metadata" not in columns, "Original nested metadata column should be removed"

            # Verify flattened values
            row = transformed_df.collect()[0]
            assert row["metadata_source"] == "external_api"
            assert row["metadata_priority"] == "high"

    @pytest.mark.integration
    def test_api_data_fetch_error_handling(self, mock_job_context, job_factory):
        """Test error handling for API failures"""

        # Mock API client that raises an exception
        mock_api_client = Mock()
        mock_api_client.get.side_effect = Exception("API connection failed")

        with mock_job_context as mocks:
            job = job_factory.create_job(
                APIDataFetchJob,
                "api_data_fetch",
                "test-run-123",
                {"env": "dev", "api_endpoint": "/api/v1/error"}
            )

            job.api_client = mock_api_client

            # Should raise exception during extraction
            with pytest.raises(Exception) as exc_info:
                job.extract()

            assert "API connection failed" in str(exc_info.value)

    @pytest.mark.integration
    def test_api_data_fetch_empty_response(self, mock_job_context, job_factory):
        """Test handling of empty API responses"""

        # Mock empty API response
        empty_response = {"data": [], "pagination": {"page": 1, "total_pages": 1}}

        mock_api_client = Mock()
        mock_api_client.get.return_value = empty_response

        with mock_job_context as mocks:
            job = job_factory.create_job(
                APIDataFetchJob,
                "api_data_fetch",
                "test-run-123",
                {"env": "dev", "api_endpoint": "/api/v1/empty"}
            )

            job.api_client = mock_api_client

            # Extract should return empty DataFrame
            raw_df = job.extract()
            assert raw_df.count() == 0, "Should return empty DataFrame for empty API response"

            # Transform should handle empty input gracefully
            transformed_df = job.transform(raw_df)
            assert transformed_df.count() == 0, "Should handle empty DataFrame transformation"

            # Validation should fail for empty dataset
            validation_result = job.validate(transformed_df)
            assert validation_result is False, "Validation should fail for empty dataset"

    @pytest.mark.integration
    def test_api_data_fetch_validation_failures(self, mock_job_context, job_factory):
        """Test validation failure scenarios"""

        # Create invalid API response (missing required 'id' field)
        invalid_response = {
            "data": [
                {
                    "name": "Product Without ID",
                    "value": "100.00",
                    "created_at": "2023-01-15T10:30:00Z"
                }
            ],
            "pagination": {"page": 1, "total_pages": 1}
        }

        mock_api_client = Mock()
        mock_api_client.get.return_value = invalid_response

        with mock_job_context as mocks:
            job = job_factory.create_job(
                APIDataFetchJob,
                "api_data_fetch",
                "test-run-123",
                {"env": "dev", "api_endpoint": "/api/v1/invalid"}
            )

            job.api_client = mock_api_client

            # Extract and transform
            raw_df = job.extract()
            transformed_df = job.transform(raw_df)

            # Validation should fail due to missing 'id' column
            validation_result = job.custom_validation(transformed_df)
            assert validation_result is False, "Validation should fail for missing required columns"

    @pytest.mark.integration
    def test_api_data_fetch_local_file_output(self, mock_job_context, job_factory, runner):
        """Test that local output files are created correctly"""

        # Create temporary directory for output
        output_dir = runner.get_temp_dir()

        # Create minimal test data
        test_response = {
            "data": [
                {
                    "id": "local_001",
                    "name": "Local Test Product",
                    "value": "50.00",
                    "created_at": "2023-01-15T10:30:00Z"
                }
            ],
            "pagination": {"page": 1, "total_pages": 1}
        }

        mock_api_client = Mock()
        mock_api_client.get.return_value = test_response

        with mock_job_context as mocks:
            job = job_factory.create_job(
                APIDataFetchJob,
                "api_data_fetch",
                "test-run-123",
                {"env": "local", "api_endpoint": "/api/v1/local"}
            )

            job.api_client = mock_api_client

            # Extract and transform
            raw_df = job.extract()
            transformed_df = job.transform(raw_df)

            # Mock the local output path to use our temp directory
            def mock_load(df):
                output_path = os.path.join(output_dir, "api_data/")
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

    @pytest.mark.integration
    def test_api_data_fetch_pagination_limit(self, mock_job_context, job_factory):
        """Test that pagination respects max_pages limit"""

        # Create responses for multiple pages
        def create_page_response(page_num):
            return {
                "data": [{"id": f"page_{page_num}_001", "name": f"Product Page {page_num}"}],
                "pagination": {"page": page_num, "total_pages": 10}  # Claim 10 pages available
            }

        mock_api_client = Mock()
        mock_api_client.get.side_effect = [create_page_response(i) for i in range(1, 11)]

        with mock_job_context as mocks:
            job = job_factory.create_job(
                APIDataFetchJob,
                "api_data_fetch",
                "test-run-123",
                {
                    "env": "dev",
                    "api_endpoint": "/api/v1/paginated",
                    "page_size": "1",
                    "max_pages": "3"  # Limit to 3 pages
                }
            )

            job.api_client = mock_api_client

            # Extract should only fetch 3 pages despite 10 being available
            raw_df = job.extract()
            assert raw_df.count() == 3, "Should only extract 3 records (respecting max_pages limit)"

            # Verify only 3 API calls were made
            assert mock_api_client.get.call_count == 3, "Should only make 3 API calls"
