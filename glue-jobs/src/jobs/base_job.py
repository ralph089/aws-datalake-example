"""
Simple base class for AWS Glue ETL jobs.

Provides core ETL patterns with logging, notifications, and secrets management.
"""

from abc import ABC, abstractmethod
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from config import JobConfig
from utils.api_client import APIClient
from utils.logging import setup_logging
from utils.notifications import NotificationService
from utils.secrets import get_secret


class BaseGlueJob(ABC):
    """
    Base class for AWS Glue ETL jobs following extract → transform → validate → load pattern.

    Features:
    - Dual-mode processing: S3 event-triggered or scheduled
    - Structured logging
    - SNS notifications
    - AWS Secrets Manager integration
    - Apache Iceberg table support
    """

    def __init__(self, config: JobConfig):
        """Initialize job with configuration."""
        self.config = config
        self.job_name = config.job_name
        self.job_run_id = config.job_run_id

        # Setup logging
        self.logger = setup_logging(self.job_name, config.log_level)
        self.logger.info(f"Starting job {self.job_name} (run_id: {self.job_run_id})")
        self.logger.info(f"Trigger type: {config.trigger_type}")

        # Initialize services
        self.notification_service: NotificationService | None
        if config.enable_notifications:
            self.notification_service = NotificationService(config.env)
        else:
            self.notification_service = None

        self.api_client: APIClient | None = None  # Initialized when needed
        self.spark: SparkSession | None = None

    def run(self) -> bool:
        """Execute the complete ETL pipeline."""
        try:
            self.logger.info("Starting ETL pipeline")

            # Initialize Spark
            self._initialize_spark()

            # Execute ETL steps
            raw_data = self.extract()
            if raw_data is None or raw_data.count() == 0:
                self.logger.warning("No data extracted, skipping processing")
                return True

            transformed_data = self.transform(raw_data)

            if not self.validate(transformed_data):
                raise RuntimeError("Data validation failed")

            self.load(transformed_data)

            self.logger.info("ETL pipeline completed successfully")
            self._send_notification("SUCCESS", "Job completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Job failed: {str(e)}")
            self._send_notification("FAILED", f"Job failed: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

    @abstractmethod
    def extract(self) -> DataFrame | None:
        """Extract data from source(s). Return None if no data to process."""
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform the data."""
        pass

    def validate(self, df: DataFrame) -> bool:
        """Validate the data. Override for custom validation."""
        if df is None:
            return False

        row_count = df.count()
        self.logger.info(f"Data validation: {row_count} rows")

        if row_count == 0:
            self.logger.warning("No data to validate")
            return False

        # Basic validation - check for required columns
        required_columns = self._get_required_columns()
        if required_columns:
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                self.logger.error(f"Missing required columns: {missing_columns}")
                return False

        return True

    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """Load data to destination."""
        pass

    def _get_required_columns(self) -> list[str]:
        """Get list of required columns for validation. Override in subclasses."""
        return []

    def _initialize_spark(self) -> None:
        """Initialize Spark session with optimized settings."""
        self.spark = (
            SparkSession.builder.appName(self.job_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )

        self.logger.info("Spark session initialized")

    def _get_data_path(self, base_path: str, file_pattern: str = "*") -> str:
        """
        Get data path based on trigger type.

        For S3 events: use specific file
        For scheduled: use directory pattern
        """
        if self.config.is_event_triggered:
            # Use specific file from S3 event
            return f"s3://{self.config.bucket}/{self.config.object_key}"
        else:
            # Use directory pattern for scheduled runs
            return f"{base_path}/{file_pattern}"

    def load_data(self, path: str, file_format: str = "csv") -> DataFrame:
        """Load data from file path with automatic format detection."""
        self.logger.info(f"Loading data from: {path}")

        if self.spark is None:
            raise RuntimeError("Spark session not initialized")

        if file_format.lower() == "csv":
            return (
                self.spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(path)
            )
        elif file_format.lower() in ["dat", "tsv"]:
            return (
                self.spark.read.option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", "\t")
                .csv(path)
            )
        elif file_format.lower() == "json":
            return self.spark.read.json(path)
        elif file_format.lower() == "parquet":
            return self.spark.read.parquet(path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

    def load_test_data(self, job_name: str, filename: str) -> DataFrame:
        """Load test data for local development."""
        if self.config.env != "local":
            raise RuntimeError("Test data loading only available in local environment")

        test_data_path = (
            Path(__file__).parent.parent.parent / "test_data" / job_name / filename
        )

        if not test_data_path.exists():
            raise FileNotFoundError(f"Test data file not found: {test_data_path}")

        file_format = test_data_path.suffix[1:]  # Remove the dot
        return self.load_data(str(test_data_path), file_format)

    def get_api_client(self, secret_name: str) -> APIClient:
        """Get API client with credentials from AWS Secrets Manager."""
        if not self.api_client:
            credentials = get_secret(secret_name)
            if credentials is None:
                raise ValueError(
                    f"Failed to retrieve credentials from secret: {secret_name}"
                )
            self.api_client = APIClient(
                base_url=credentials["api_base_url"],
                client_id=credentials["client_id"],
                client_secret=credentials["client_secret"],
                token_endpoint=credentials.get("token_endpoint", "/oauth/token"),
            )

        return self.api_client

    def write_to_iceberg(
        self, df: DataFrame, table_name: str, mode: str = "overwrite"
    ) -> None:
        """Write DataFrame to Iceberg table."""
        self.logger.info(f"Writing to Iceberg table: {table_name}")

        # Iceberg table configuration
        writer = df.write.format("iceberg").mode(mode)

        if self.config.env != "local":
            # Use Glue catalog in AWS environments
            writer = writer.option("catalog", "glue_catalog")

        writer.saveAsTable(table_name)

        self.logger.info(f"Successfully wrote {df.count()} rows to {table_name}")

    def _send_notification(self, status: str, message: str) -> None:
        """Send job status notification."""
        if not self.notification_service:
            return

        try:
            job_data = {
                "job_name": self.job_name,
                "job_run_id": self.job_run_id,
                "environment": self.config.env,
                "trigger_type": self.config.trigger_type,
                "message": message,
            }

            if status == "SUCCESS":
                self.notification_service.send_success_notification(job_data)
            else:
                job_data["error"] = message
                self.notification_service.send_failure_notification(job_data)

        except Exception as e:
            self.logger.warning(f"Failed to send notification: {e}")
