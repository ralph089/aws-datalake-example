"""
Tests for Lambda handler function.
"""

import json
from unittest.mock import Mock, patch

import pytest

from handler import _infer_table_from_s3_key, _parse_event, lambda_handler


class TestLambdaHandler:
    """Test cases for Lambda handler."""

    @pytest.mark.unit
    @patch("handler._send_to_api")
    @patch("handler.DataLakeConnector")
    def test_lambda_handler_scheduled_event_success(
        self, mock_connector_class, mock_send_to_api, scheduled_event, mock_context, sample_athena_records
    ):
        """Test successful processing of scheduled event."""
        # Mock data connector
        mock_connector = Mock()
        mock_connector.build_query.return_value = "SELECT * FROM dev_customers_silver"
        mock_connector.query_table.return_value = sample_athena_records
        mock_connector_class.return_value = mock_connector

        # Mock API send
        mock_send_to_api.return_value = {"total_records": 3, "successful_batches": 1, "failed_batches": 0}

        # Execute handler
        result = lambda_handler(scheduled_event, mock_context)

        # Verify response
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["recordsProcessed"] == 3
        assert "Data processed successfully" in body["message"]

        # Verify mocks called correctly
        mock_connector.build_query.assert_called_once()
        mock_connector.query_table.assert_called_once()
        # Verify mock_send_to_api was called with the right data
        mock_send_to_api.assert_called_once()
        call_args = mock_send_to_api.call_args[0]
        assert call_args[1] == sample_athena_records  # records parameter

    @pytest.mark.unit
    @patch("handler.DataLakeConnector")
    def test_lambda_handler_no_data_found(self, mock_connector_class, scheduled_event, mock_context):
        """Test handler when no data is found."""
        # Mock empty data response
        mock_connector = Mock()
        mock_connector.build_query.return_value = "SELECT * FROM dev_customers_silver"
        mock_connector.query_table.return_value = []
        mock_connector_class.return_value = mock_connector

        result = lambda_handler(scheduled_event, mock_context)

        # Verify 204 response for no data
        assert result["statusCode"] == 204
        body = json.loads(result["body"])
        assert body["recordsProcessed"] == 0
        assert "No data found" in body["message"]

    @pytest.mark.unit
    @patch("handler.DataLakeConnector")
    def test_lambda_handler_error_handling(self, mock_connector_class, scheduled_event, mock_context):
        """Test error handling in Lambda handler."""
        # Mock exception
        mock_connector_class.side_effect = Exception("Database connection failed")

        result = lambda_handler(scheduled_event, mock_context)

        # Verify error response
        assert result["statusCode"] == 500
        body = json.loads(result["body"])
        assert "Database connection failed" in body["error"]
        assert "Lambda execution failed" in body["message"]

    @pytest.mark.unit
    @patch("handler._send_to_api")
    @patch("handler.DataLakeConnector")
    def test_lambda_handler_s3_event(
        self, mock_connector_class, mock_send_to_api, s3_event, mock_context, sample_athena_records
    ):
        """Test S3 event processing."""
        # Mock data connector
        mock_connector = Mock()
        mock_connector.build_query.return_value = "SELECT * FROM dev_customers_silver"
        mock_connector.query_table.return_value = sample_athena_records
        mock_connector_class.return_value = mock_connector

        # Mock API send
        mock_send_to_api.return_value = {"total_records": 3}

        result = lambda_handler(s3_event, mock_context)

        assert result["statusCode"] == 200

        # Verify query built for inferred table
        mock_connector.build_query.assert_called_once()
        call_args = mock_connector.build_query.call_args
        assert call_args[1]["table_name"] == "dev_customers_silver"
        assert call_args[1]["date_filter"] == "today"

    @pytest.mark.unit
    @patch("handler._send_to_api")
    @patch("handler.DataLakeConnector")
    def test_lambda_handler_api_gateway_event(
        self, mock_connector_class, mock_send_to_api, api_gateway_event, mock_context, sample_athena_records
    ):
        """Test API Gateway event processing."""
        # Mock data connector
        mock_connector = Mock()
        mock_connector.build_query.return_value = "SELECT * FROM dev_customers_silver WHERE customer_id > '100'"
        mock_connector.query_table.return_value = sample_athena_records
        mock_connector_class.return_value = mock_connector

        # Mock API send
        mock_send_to_api.return_value = {"total_records": 3}

        result = lambda_handler(api_gateway_event, mock_context)

        assert result["statusCode"] == 200

        # Verify query built with API parameters
        mock_connector.build_query.assert_called_once()
        call_args = mock_connector.build_query.call_args
        assert call_args[1]["table_name"] == "dev_customers_silver"
        assert call_args[1]["where_clause"] == "customer_id > '100'"
        assert call_args[1]["limit"] == 100


class TestEventParsing:
    """Test event parsing functions."""

    @pytest.mark.unit
    def test_parse_event_scheduled(self, scheduled_event):
        """Test parsing of scheduled event."""
        event_type, config = _parse_event(scheduled_event)

        assert event_type == "scheduled"
        assert config.table_name == "dev_customers_silver"
        assert config.date_filter == "yesterday"

    @pytest.mark.unit
    def test_parse_event_s3(self, s3_event):
        """Test parsing of S3 event."""
        event_type, config = _parse_event(s3_event)

        assert event_type == "s3"
        assert config.bucket == "data-lake"
        assert config.object_key == "silver/customers/part-001.parquet"

    @pytest.mark.unit
    def test_parse_event_api_gateway(self, api_gateway_event):
        """Test parsing of API Gateway event."""
        event_type, config = _parse_event(api_gateway_event)

        assert event_type == "api_gateway"
        assert config.table_name == "dev_customers_silver"
        assert config.limit == 100
        assert config.where_clause == "customer_id > '100'"

    @pytest.mark.unit
    def test_parse_event_unknown(self):
        """Test parsing of unknown event type."""
        unknown_event = {"unknown": "event"}

        event_type, config = _parse_event(unknown_event)

        # Should default to scheduled
        assert event_type == "scheduled"
        assert hasattr(config, "table_name")


class TestUtilityFunctions:
    """Test utility functions."""

    @pytest.mark.unit
    def test_infer_table_from_s3_key(self):
        """Test table name inference from S3 key."""
        test_cases = [
            ("silver/customers/data.parquet", "dev_customers_silver"),
            ("bronze/products/raw.json", "dev_products_bronze"),
            ("gold/orders/aggregated.parquet", "dev_orders_gold"),
            ("invalid", "dev_customers_silver"),  # fallback
        ]

        for s3_key, expected in test_cases:
            with patch("handler.create_lambda_config") as mock_config:
                mock_config.return_value.env = "dev"
                result = _infer_table_from_s3_key(s3_key)
                assert result == expected

    @pytest.mark.unit
    @patch("handler.APIClient")
    def test_send_to_api_success(self, mock_api_client_class, lambda_config, sample_athena_records):
        """Test successful API send."""
        from handler import _send_to_api

        # Mock API client
        mock_client = Mock()
        mock_client.send_batch.return_value = {"total_records": 3, "successful_batches": 1, "failed_batches": 0}
        mock_api_client_class.from_secrets_manager.return_value.__enter__ = Mock(return_value=mock_client)
        mock_api_client_class.from_secrets_manager.return_value.__exit__ = Mock(return_value=None)

        result = _send_to_api(lambda_config, sample_athena_records)

        assert result["total_records"] == 3
        assert result["successful_batches"] == 1
        mock_client.send_batch.assert_called_once()

    @pytest.mark.unit
    @patch("handler.APIClient")
    def test_send_to_api_failure(self, mock_api_client_class, lambda_config, sample_athena_records):
        """Test API send failure."""
        from handler import _send_to_api

        # Mock API client to raise exception
        mock_api_client_class.from_secrets_manager.side_effect = Exception("API connection failed")

        with pytest.raises(Exception, match="API connection failed"):
            _send_to_api(lambda_config, sample_athena_records)
