"""
Tests for SNS to Glue trigger Lambda handler.
"""

import json
import os

# Add src to path for imports
import sys
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from handler import _determine_glue_job, _extract_s3_events, lambda_handler


class TestSNSLambdaHandler:
    """Test cases for the SNS Lambda handler."""

    @patch("handler._get_glue_client")
    def test_lambda_handler_success(self, mock_get_glue_client):
        """Test successful Lambda handler execution."""
        # Mock Glue client response
        mock_glue_client = Mock()
        mock_glue_client.start_job_run.return_value = {"JobRunId": "jr_test123"}
        mock_get_glue_client.return_value = mock_glue_client

        # Sample SNS event
        event = {
            "Records": [
                {
                    "EventSource": "aws:sns",
                    "Sns": {
                        "Message": json.dumps(
                            {
                                "Records": [
                                    {
                                        "eventSource": "aws:s3",
                                        "eventName": "ObjectCreated:Put",
                                        "s3": {"bucket": {"name": "test-bucket"}, "object": {"key": "sales/data.csv"}},
                                    }
                                ]
                            }
                        )
                    },
                }
            ]
        }

        with patch.dict(os.environ, {"ENVIRONMENT": "test"}):
            response = lambda_handler(event, {})

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert "Processed 1 SNS records" in body["message"]
        assert len(body["results"]) == 1
        assert body["results"][0]["status"] == "success"

    def test_extract_s3_events_direct_format(self):
        """Test extracting S3 events from direct S3 event format."""
        sns_message = {
            "Records": [
                {
                    "eventSource": "aws:s3",
                    "eventName": "ObjectCreated:Put",
                    "s3": {"bucket": {"name": "test-bucket"}, "object": {"key": "sales/data.csv"}},
                }
            ]
        }

        events = _extract_s3_events(sns_message)

        assert len(events) == 1
        assert events[0]["bucket"] == "test-bucket"
        assert events[0]["object_key"] == "sales/data.csv"
        assert events[0]["event_name"] == "ObjectCreated:Put"

    def test_extract_s3_events_eventbridge_format(self):
        """Test extracting S3 events from EventBridge format."""
        sns_message = {
            "detail-type": "Object Created",
            "detail": {"bucket": {"name": "test-bucket"}, "object": {"key": "customers/data.json"}},
        }

        events = _extract_s3_events(sns_message)

        assert len(events) == 1
        assert events[0]["bucket"] == "test-bucket"
        assert events[0]["object_key"] == "customers/data.json"
        assert events[0]["event_name"] == "Object Created"

    def test_extract_s3_events_simple_format(self):
        """Test extracting S3 events from simple key-value format."""
        sns_message = {"bucket": "test-bucket", "object_key": "inventory/items.csv", "event_name": "ObjectCreated"}

        events = _extract_s3_events(sns_message)

        assert len(events) == 1
        assert events[0]["bucket"] == "test-bucket"
        assert events[0]["object_key"] == "inventory/items.csv"
        assert events[0]["event_name"] == "ObjectCreated"

    def test_determine_glue_job_default_mappings(self):
        """Test Glue job determination with default mappings."""
        with patch.dict(os.environ, {"ENVIRONMENT": "test"}, clear=True):
            # Test sales prefix
            job = _determine_glue_job("sales/2024/data.csv")
            assert job == "test-sales_etl"

            # Test customers prefix
            job = _determine_glue_job("customers/new_customers.json")
            assert job == "test-customer_import"

            # Test inventory prefix
            job = _determine_glue_job("inventory/stock.csv")
            assert job == "test-inventory_sync"

            # Test products prefix
            job = _determine_glue_job("products/catalog.json")
            assert job == "test-product_catalog"

            # Test no match
            job = _determine_glue_job("unknown/data.csv")
            assert job is None

    def test_determine_glue_job_custom_mappings(self):
        """Test Glue job determination with custom mappings."""
        custom_mappings = "data/:custom-job,reports/:report-job"

        with patch.dict(os.environ, {"ENVIRONMENT": "test", "GLUE_JOB_MAPPINGS": custom_mappings}):
            # Test custom mappings
            job = _determine_glue_job("data/input.csv")
            assert job == "custom-job"

            job = _determine_glue_job("reports/monthly.json")
            assert job == "report-job"

            # Test longest prefix match
            custom_mappings_long = "data/:job1,data/special/:job2"
            with patch.dict(os.environ, {"GLUE_JOB_MAPPINGS": custom_mappings_long}):
                job = _determine_glue_job("data/special/file.csv")
                assert job == "job2"  # Longest prefix wins

    @patch("handler._get_glue_client")
    def test_trigger_glue_job(self, mock_get_glue_client):
        """Test Glue job triggering."""
        from handler import _trigger_glue_job

        # Mock Glue client response
        mock_glue_client = Mock()
        mock_glue_client.start_job_run.return_value = {"JobRunId": "jr_test123"}
        mock_get_glue_client.return_value = mock_glue_client

        with patch.dict(os.environ, {"ENVIRONMENT": "test"}):
            job_run_id = _trigger_glue_job("test-job", "test-bucket", "data/file.csv")

        assert job_run_id == "jr_test123"
        mock_glue_client.start_job_run.assert_called_once_with(
            JobName="test-job",
            Arguments={
                "--bucket": "test-bucket",
                "--object_key": "data/file.csv",
                "--env": "test",
                "--enable_metrics": "true",
            },
        )

    def test_extract_s3_events_empty_message(self):
        """Test extracting events from empty or invalid SNS message."""
        # Empty message
        events = _extract_s3_events({})
        assert len(events) == 0

        # Invalid structure
        events = _extract_s3_events({"invalid": "data"})
        assert len(events) == 0

        # None message
        events = _extract_s3_events(None)
        assert len(events) == 0

    def test_lambda_handler_no_s3_events(self):
        """Test Lambda handler with SNS message containing no S3 events."""
        event = {"Records": [{"EventSource": "aws:sns", "Sns": {"Message": json.dumps({"invalid": "message"})}}]}

        response = lambda_handler(event, {})

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert "Processed 1 SNS records" in body["message"]
        assert body["results"][0]["status"] == "skipped"
