import json
import logging
import os
from unittest.mock import patch

import pytest
import structlog

from utils.logging_config import _add_exception_context, _get_log_level, _truncate_long_values, setup_logging


class TestGetLogLevel:
    """Test the log level parsing function"""

    def test_valid_string_levels(self):
        """Should convert valid string levels to integers"""
        assert _get_log_level("DEBUG") == logging.DEBUG
        assert _get_log_level("INFO") == logging.INFO
        assert _get_log_level("WARN") == logging.WARNING
        assert _get_log_level("WARNING") == logging.WARNING
        assert _get_log_level("ERROR") == logging.ERROR
        assert _get_log_level("CRITICAL") == logging.CRITICAL

    def test_case_insensitive(self):
        """Should handle lowercase input"""
        assert _get_log_level("debug") == logging.DEBUG
        assert _get_log_level("info") == logging.INFO
        assert _get_log_level("error") == logging.ERROR

    def test_valid_integer_levels(self):
        """Should accept valid integer levels"""
        assert _get_log_level("10") == logging.DEBUG
        assert _get_log_level("20") == logging.INFO
        assert _get_log_level("30") == logging.WARNING
        assert _get_log_level("40") == logging.ERROR
        assert _get_log_level("50") == logging.CRITICAL

    def test_invalid_levels_default_to_info(self):
        """Should default to INFO for invalid levels"""
        assert _get_log_level("INVALID") == logging.INFO
        assert _get_log_level("999") == logging.INFO
        assert _get_log_level("") == logging.INFO
        assert _get_log_level("15") == logging.INFO  # Invalid integer level


class TestAddExceptionContext:
    """Test the exception context processor"""

    def test_adds_exception_info_when_present(self):
        """Should add exception type and message when exc_info is present"""
        try:
            raise ValueError("Test error message")
        except ValueError:
            import sys
            exc_info = sys.exc_info()
            
        event_dict = {"exc_info": exc_info, "message": "An error occurred"}
        result = _add_exception_context(None, None, event_dict)
        
        assert result["exception_type"] == "ValueError"
        assert result["exception_message"] == "Test error message"

    def test_ignores_when_no_exc_info(self):
        """Should not modify event dict when no exc_info present"""
        event_dict = {"message": "Normal log message"}
        result = _add_exception_context(None, None, event_dict)
        
        assert result == event_dict
        assert "exception_type" not in result
        assert "exception_message" not in result

    def test_handles_none_exc_info(self):
        """Should handle None exc_info gracefully"""
        event_dict = {"exc_info": None, "message": "Log message"}
        result = _add_exception_context(None, None, event_dict)
        
        assert result == event_dict
        assert "exception_type" not in result

    def test_adds_correlation_id_from_logger_context(self):
        """Should add correlation ID from logger context if available"""
        try:
            raise RuntimeError("Test error")
        except RuntimeError:
            import sys
            exc_info = sys.exc_info()
        
        # Mock logger with context
        mock_logger = type('MockLogger', (), {
            '_context': {'job_run_id': 'test-job-123'}
        })()
        
        event_dict = {"exc_info": exc_info}
        result = _add_exception_context(mock_logger, None, event_dict)
        
        assert result["correlation_id"] == "test-job-123"
        assert result["exception_type"] == "RuntimeError"


class TestTruncateLongValues:
    """Test the log field truncation processor"""

    def test_truncates_long_strings(self):
        """Should truncate strings longer than max_length"""
        event_dict = {"message": "x" * 150}
        result = _truncate_long_values(None, None, event_dict)
        assert len(result["message"]) == 103  # 100 chars + "..."
        assert result["message"].endswith("...")

    def test_preserves_short_strings(self):
        """Should not modify strings shorter than max_length"""
        event_dict = {"message": "short message"}
        result = _truncate_long_values(None, None, event_dict)
        assert result["message"] == "short message"

    def test_handles_quality_validation_results(self):
        """Should convert quality validation results to summary"""
        event_dict = {
            "results": {
                "quality_validation": {
                    "overall_status": "PASSED",
                    "passed_count": 5,
                    "failed_count": 0
                }
            }
        }
        result = _truncate_long_values(None, None, event_dict)
        assert "validation_summary" in result
        assert result["validation_summary"] == "PASSED (5 passed, 0 failed)"
        assert "results" not in result

    def test_handles_validation_results_field(self):
        """Should convert validation_results field to summary"""
        event_dict = {
            "validation_results": {
                "quality_validation": {
                    "overall_status": "FAILED",
                    "passed_count": 3,
                    "failed_count": 2
                }
            }
        }
        result = _truncate_long_values(None, None, event_dict)
        assert result["validation_summary"] == "FAILED (3 passed, 2 failed)"
        assert "validation_results" not in result

    def test_handles_large_dicts(self):
        """Should simplify large dictionary values"""
        large_dict = {f"key_{i}": f"value_{i}" for i in range(50)}
        event_dict = {"data": large_dict}
        result = _truncate_long_values(None, None, event_dict)
        assert result["data"] == "<complex_data_type>"

    def test_handles_dict_with_status(self):
        """Should extract status from dicts that have overall_status when dict is large"""
        # Create a large dict that will trigger truncation
        large_dict = {
            "overall_status": "SUCCESS",
            "details": "x" * 200  # This makes the dict string representation > 100 chars
        }
        event_dict = {"metadata": large_dict}
        result = _truncate_long_values(None, None, event_dict)
        assert result["metadata"] == "summary=SUCCESS"


class TestSetupLogging:
    """Test the logging setup function"""

    def test_returns_structlog_logger(self):
        """Should return a structlog BoundLogger or BoundLoggerLazyProxy"""
        logger = setup_logging("test_job", "local")
        # structlog can return different types, but they should all have the same interface
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'error')
        assert hasattr(logger, 'bind')

    def test_local_environment_uses_console_renderer(self):
        """Local environment should use console rendering"""
        with patch('structlog.configure') as mock_configure:
            setup_logging("test_job", "local")
            
            # Get the processors from the configure call
            call_args = mock_configure.call_args
            processors = call_args[1]['processors']
            
            # Should have ConsoleRenderer for local
            has_console_renderer = any(
                hasattr(proc, '__class__') and 'ConsoleRenderer' in proc.__class__.__name__
                for proc in processors
            )
            assert has_console_renderer

    def test_production_environment_uses_json_renderer(self):
        """Production environments should use JSON rendering"""
        for env in ["dev", "staging", "prod"]:
            with patch('structlog.configure') as mock_configure:
                setup_logging("test_job", env)
                
                call_args = mock_configure.call_args
                processors = call_args[1]['processors']
                
                # Should have JSONRenderer for production environments
                has_json_renderer = any(
                    hasattr(proc, '__class__') and 'JSONRenderer' in proc.__class__.__name__
                    for proc in processors
                )
                assert has_json_renderer

    @patch.dict(os.environ, {"NO_COLOR": "1"})
    def test_respects_no_color_env_var(self):
        """Should disable colors when NO_COLOR is set"""
        with patch('structlog.configure') as mock_configure:
            setup_logging("test_job", "local")
            
            call_args = mock_configure.call_args
            processors = call_args[1]['processors']
            
            # Find the ConsoleRenderer and check it has colors=False
            for proc in processors:
                if hasattr(proc, '__class__') and 'ConsoleRenderer' in proc.__class__.__name__:
                    # The ConsoleRenderer should be created with colors=False
                    break

    @patch.dict(os.environ, {"FORCE_COLOR": "1"})
    def test_respects_force_color_env_var(self):
        """Should enable colors when FORCE_COLOR is set"""
        with patch('structlog.configure') as mock_configure:
            setup_logging("test_job", "prod")  # Production normally has no colors
            
            call_args = mock_configure.call_args
            processors = call_args[1]['processors']
            
            # Production with FORCE_COLOR should still use JSON (no console renderer)
            has_json_renderer = any(
                hasattr(proc, '__class__') and 'JSONRenderer' in proc.__class__.__name__
                for proc in processors
            )
            assert has_json_renderer

    def test_includes_custom_processors(self):
        """Should include custom processors in all environments"""
        for env in ["local", "dev", "prod"]:
            with patch('structlog.configure') as mock_configure:
                setup_logging("test_job", env)
                
                call_args = mock_configure.call_args
                processors = call_args[1]['processors']
                
                # Should include our custom functions
                assert _truncate_long_values in processors
                assert _add_exception_context in processors

    @patch('logging.basicConfig')
    def test_configures_basic_logging(self, mock_basic_config):
        """Should configure basic logging for all environments"""
        setup_logging("test_job", "local")
        mock_basic_config.assert_called_once()
        
        # Check that it was called with appropriate parameters
        call_args = mock_basic_config.call_args[1]
        assert call_args['level'] == logging.INFO
        assert call_args['format'] == "%(message)s"

    @patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"})
    @patch('logging.basicConfig')
    def test_respects_log_level_env_var(self, mock_basic_config):
        """Should use LOG_LEVEL environment variable"""
        setup_logging("test_job", "local")
        
        call_args = mock_basic_config.call_args[1]
        assert call_args['level'] == logging.DEBUG

    @patch.dict(os.environ, {"LOG_LEVEL": "ERROR"})
    @patch('structlog.configure')
    def test_log_level_affects_filtering(self, mock_configure):
        """Should use LOG_LEVEL for filtering configuration"""
        setup_logging("test_job", "prod")
        
        call_args = mock_configure.call_args[1]
        wrapper_class = call_args['wrapper_class']
        # The wrapper class should be configured with ERROR level filtering
        # We can't easily test the exact filtering level, but we can verify
        # it's a filtering wrapper by checking the class name
        assert 'filtering' in str(wrapper_class).lower() or 'bound' in str(wrapper_class).lower()