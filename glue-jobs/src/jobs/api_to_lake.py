"""
API to Data Lake ETL job example.

Demonstrates API data fetching with OAuth authentication and data lake storage.
"""

import json
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import col, explode, from_json, length, when
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from config import create_config_from_glue_args, create_local_config
from jobs.base_job import BaseGlueJob
from transformations import add_processing_metadata


# Helper functions for typed column operations
def column_less_than(column: Column, value: int) -> Column:
    """Helper to create typed column comparison."""
    return column < value  # type: ignore[operator]


def column_greater_than(column: Column, value: int) -> Column:
    """Helper to create typed column comparison."""
    return column > value  # type: ignore[operator]


def column_is_not_null(column: Column) -> Column:
    """Helper to create typed null check."""
    return column.isNotNull()  # type: ignore[arg-type]


def columns_and(left: Column, right: Column) -> Column:
    """Helper to create typed boolean AND operation."""
    return left & right  # type: ignore[operator]


class APIToLakeJob(BaseGlueJob):
    """
    Example job that fetches data from REST API and loads to data lake.

    Pipeline:
    1. Extract: Fetch data from REST API using OAuth authentication
    2. Transform: Normalize JSON data and flatten nested structures
    3. Validate: Check API response structure and data completeness
    4. Load: Write to Iceberg table in data lake
    """

    # Define expected API response schema
    PRODUCT_SCHEMA = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DecimalType(10, 2), True),
            StructField("description", StringType(), True),
        ]
    )

    def extract(self) -> DataFrame | None:
        """Extract product data from REST API."""
        if self.config.env == "local":
            # Load mock API response for local development
            return self._load_mock_api_data()
        else:
            # Fetch from real API in AWS environments
            return self._fetch_from_api()

    def _load_mock_api_data(self) -> DataFrame:
        """Load mock API response from test data."""
        test_file = (
            Path(__file__).parent.parent.parent
            / "test_data"
            / "api_to_lake"
            / "products_api.json"
        )

        if not test_file.exists():
            raise FileNotFoundError(f"Test API data not found: {test_file}")

        # Read JSON file and create DataFrame
        with open(test_file) as f:
            api_response = json.load(f)

        # Convert to DataFrame
        if self.spark is None:
            raise RuntimeError("Spark session not initialized")
        api_df = self.spark.createDataFrame([{"response": json.dumps(api_response)}])

        return self._parse_api_response(api_df)

    def _fetch_from_api(self) -> DataFrame:
        """Fetch data from REST API using OAuth authentication."""
        self.logger.info("Fetching data from products API")

        # Get API client with credentials from Secrets Manager
        api_client = self.get_api_client("prod/api/credentials")

        # Fetch all products with pagination
        all_products = []
        page = 1
        page_size = 100

        while True:
            response = api_client.get(
                "/api/v1/products", params={"page": page, "page_size": page_size}
            )

            products = response.get("data", [])
            if not products:
                break

            all_products.extend(products)
            self.logger.info(f"Fetched page {page}: {len(products)} products")

            # Check if there are more pages
            if len(products) < page_size:
                break

            page += 1

        self.logger.info(f"Total products fetched: {len(all_products)}")

        # Create DataFrame from API response
        api_response = {"data": all_products}
        if self.spark is None:
            raise RuntimeError("Spark session not initialized")
        api_df = self.spark.createDataFrame([{"response": json.dumps(api_response)}])

        return self._parse_api_response(api_df)

    def _parse_api_response(self, api_df: DataFrame) -> DataFrame:
        """Parse JSON API response into structured DataFrame."""
        from pyspark.sql.types import ArrayType

        # Parse JSON response - data field contains array of products
        json_schema = StructType(
            [StructField("data", ArrayType(self.PRODUCT_SCHEMA), True)]
        )

        # Extract products from JSON
        products_df = (
            api_df.select(from_json(col("response"), json_schema).alias("parsed"))
            .select(explode(col("parsed.data")).alias("product"))
            .select("product.*")
        )

        return products_df

    def transform(self, df: DataFrame) -> DataFrame:
        """Transform and enrich API data."""
        self.logger.info("Transforming API data")

        # Add derived fields
        price_col = col("price")
        name_col = col("name")

        transformed_df = (
            df.withColumn(
                "price_category",
                when(column_less_than(price_col, 10), "budget")
                .when(column_less_than(price_col, 50), "mid_range")
                .otherwise("premium"),
            )
            .withColumn("name_length", length(name_col))
            .filter(
                columns_and(column_is_not_null(name_col), column_is_not_null(price_col))
            )
        )

        # Add processing metadata
        transformed_df = add_processing_metadata(
            transformed_df, self.job_name, self.job_run_id
        )

        self.logger.info(f"Transformed {transformed_df.count()} product records")
        return transformed_df

    def validate(self, df: DataFrame) -> bool:
        """Validate API data quality."""
        if not super().validate(df):
            return False

        # API-specific validation
        total_count = df.count()

        # Check required fields
        id_col = col("id")
        name_col = col("name")
        price_col = col("price")

        valid_ids = df.filter(column_is_not_null(id_col)).count()
        valid_names = df.filter(column_is_not_null(name_col)).count()
        valid_prices = df.filter(
            columns_and(
                column_is_not_null(price_col), column_greater_than(price_col, 0)
            )
        ).count()

        # Calculate quality metrics
        id_validity = valid_ids / total_count if total_count > 0 else 0
        name_validity = valid_names / total_count if total_count > 0 else 0
        price_validity = valid_prices / total_count if total_count > 0 else 0

        self.logger.info("API data quality metrics:")
        self.logger.info(f"  Total records: {total_count}")
        self.logger.info(f"  Valid IDs: {id_validity:.1%}")
        self.logger.info(f"  Valid names: {name_validity:.1%}")
        self.logger.info(f"  Valid prices: {price_validity:.1%}")

        # Validation rules
        if total_count < 10:
            self.logger.error(f"Too few records returned from API: {total_count}")
            return False

        if id_validity < 0.95:
            self.logger.error(f"ID validity too low: {id_validity:.1%}")
            return False

        if name_validity < 0.90:
            self.logger.error(f"Name validity too low: {name_validity:.1%}")
            return False

        return True

    def load(self, df: DataFrame) -> None:
        """Load API data to data lake."""
        table_name = f"{self.config.env}_products_bronze"

        if self.config.env == "local":
            # Show data in local environment
            self.logger.info("Local environment - showing sample API data:")
            df.show(20, truncate=False)
            df.printSchema()
            self.logger.info(f"Would write {df.count()} rows to table: {table_name}")
        else:
            # Write to Iceberg table
            self.write_to_iceberg(df, table_name)

    def _get_required_columns(self) -> list[str]:
        """Define required columns for product API data."""
        return ["id", "name", "price"]


def main() -> None:
    """Main entry point for the API ETL job."""
    if len(sys.argv) < 2:
        # Local development
        config = create_local_config("api_to_lake")
    else:
        # AWS Glue environment
        config = create_config_from_glue_args(sys.argv)

    job = APIToLakeJob(config)
    success = job.run()

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
