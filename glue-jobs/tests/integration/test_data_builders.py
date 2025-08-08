"""
Test data builders for consistent test data creation across integration tests.

Provides builders for creating test data with realistic business scenarios,
edge cases, and validation scenarios.
"""

from datetime import datetime, timedelta
from typing import Any


class CustomerDataBuilder:
    """Builder for creating customer test data with various scenarios."""

    def __init__(self):
        self.reset()

    def reset(self):
        self._customers = []
        return self

    def add_valid_customer(
        self,
        customer_id: str = "CUST001",
        first_name: str = "John",
        last_name: str = "Doe",
        email: str = "john.doe@example.com",
        phone: str = "555-1234",
        registration_date: str = "2023-01-15",
    ):
        """Add a valid customer record."""
        self._customers.append(
            {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "phone": phone,
                "registration_date": registration_date,
            }
        )
        return self

    def add_invalid_email_customer(self, customer_id: str = "CUST_BAD_EMAIL"):
        """Add customer with invalid email for testing validation."""
        self._customers.append(
            {
                "customer_id": customer_id,
                "first_name": "Invalid",
                "last_name": "Email",
                "email": "not-an-email",
                "phone": "555-0000",
                "registration_date": "2023-01-01",
            }
        )
        return self

    def add_duplicate_customer(self, customer_id: str = "CUST001"):
        """Add duplicate customer for testing uniqueness validation."""
        existing = next((c for c in self._customers if c["customer_id"] == customer_id), None)
        if existing:
            self._customers.append(existing.copy())
        return self

    def add_customers_needing_normalization(self):
        """Add customers with data that needs standardization."""
        self._customers.extend(
            [
                {
                    "customer_id": "CUST_NORMALIZE_1",
                    "first_name": "  alice  ",  # Extra whitespace
                    "last_name": "  WILLIAMS  ",  # Mixed case
                    "email": "ALICE.WILLIAMS@EXAMPLE.COM",  # Uppercase email
                    "phone": "555.123.4567",  # Dots in phone
                    "registration_date": "2023-04-05",
                },
                {
                    "customer_id": "CUST_NORMALIZE_2",
                    "first_name": "bob",
                    "last_name": "johnson",
                    "email": "Bob.Johnson@Example.Com",
                    "phone": "(555) 987-6543",  # Formatted phone
                    "registration_date": "2023-04-06",
                },
            ]
        )
        return self

    def build(self):
        """Return list of customer records."""
        return self._customers.copy()

    def build_tuples(self):
        """Return customer data as tuples for Spark DataFrame creation."""
        return [
            (c["customer_id"], c["first_name"], c["last_name"], c["email"], c["phone"], c["registration_date"])
            for c in self._customers
        ]

    def build_schema_columns(self):
        """Return column names for Spark DataFrame schema."""
        return ["customer_id", "first_name", "last_name", "email", "phone", "registration_date"]


class SalesDataBuilder:
    """Builder for creating sales transaction test data."""

    def __init__(self):
        self.reset()

    def reset(self):
        self._transactions = []
        return self

    def add_valid_transaction(
        self,
        transaction_id: str = "TXN001",
        customer_id: str = "CUST001",
        product_id: str = "PROD001",
        quantity: int = 1,
        unit_price: float = 29.99,
        transaction_date: str = "2023-01-15",
    ):
        """Add a valid sales transaction."""
        total_amount = quantity * unit_price
        self._transactions.append(
            {
                "transaction_id": transaction_id,
                "customer_id": customer_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "transaction_date": transaction_date,
            }
        )
        return self

    def add_invalid_quantity_transaction(self, transaction_id: str = "TXN_BAD_QTY"):
        """Add transaction with invalid (zero/negative) quantity."""
        self._transactions.append(
            {
                "transaction_id": transaction_id,
                "customer_id": "CUST001",
                "product_id": "PROD001",
                "quantity": 0,  # Invalid quantity
                "unit_price": 29.99,
                "total_amount": 0,
                "transaction_date": "2023-01-15",
            }
        )
        return self

    def add_daily_transactions(self, date: str = "2023-01-15", count: int = 3):
        """Add multiple transactions for the same day (for aggregation testing)."""
        base_date = datetime.fromisoformat(date)
        for i in range(count):
            transaction_id = f"TXN_{date.replace('-', '')}_{i + 1:03d}"
            customer_id = f"CUST{i + 1:03d}"
            product_id = f"PROD{(i % 2) + 1:03d}"  # Alternate between PROD001 and PROD002

            self.add_valid_transaction(
                transaction_id=transaction_id,
                customer_id=customer_id,
                product_id=product_id,
                quantity=i + 1,
                unit_price=29.99 + (i * 10),
                transaction_date=date,
            )
        return self

    def add_multi_day_customer_transactions(self, customer_id: str = "CUST001"):
        """Add transactions for same customer across multiple days (for aggregation testing)."""
        dates = ["2023-01-15", "2023-01-16", "2023-01-17"]
        for i, date in enumerate(dates):
            self.add_valid_transaction(
                transaction_id=f"TXN_MULTI_{i + 1:03d}",
                customer_id=customer_id,
                product_id=f"PROD{(i % 2) + 1:03d}",
                quantity=i + 1,
                unit_price=49.99,
                transaction_date=date,
            )
        return self

    def build(self):
        """Return list of transaction records."""
        return self._transactions.copy()

    def build_tuples(self):
        """Return transaction data as tuples for Spark DataFrame creation."""
        return [
            (
                t["transaction_id"],
                t["customer_id"],
                t["product_id"],
                t["quantity"],
                t["unit_price"],
                t["total_amount"],
                t["transaction_date"],
            )
            for t in self._transactions
        ]

    def build_schema_columns(self):
        """Return column names for Spark DataFrame schema."""
        return [
            "transaction_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "total_amount",
            "transaction_date",
        ]


class InventoryDataBuilder:
    """Builder for creating inventory test data."""

    def __init__(self):
        self.reset()

    def reset(self):
        self._inventory_records = []
        return self

    def add_well_stocked_item(
        self,
        product_id: str = "PROD001",
        location_id: str = "LOC001",
        quantity_on_hand: int = 150,
        reorder_point: int = 50,
        max_stock_level: int = 500,
    ):
        """Add inventory item that is well-stocked (above reorder point)."""
        self._inventory_records.append(
            {
                "product_id": product_id,
                "location_id": location_id,
                "quantity_on_hand": quantity_on_hand,
                "reorder_point": reorder_point,
                "max_stock_level": max_stock_level,
                "last_updated": "2023-01-15",
            }
        )
        return self

    def add_low_stock_item(
        self,
        product_id: str = "PROD002",
        location_id: str = "LOC001",
        quantity_on_hand: int = 25,
        reorder_point: int = 30,
        max_stock_level: int = 200,
    ):
        """Add inventory item that needs reordering (below reorder point)."""
        self._inventory_records.append(
            {
                "product_id": product_id,
                "location_id": location_id,
                "quantity_on_hand": quantity_on_hand,
                "reorder_point": reorder_point,
                "max_stock_level": max_stock_level,
                "last_updated": "2023-01-15",
            }
        )
        return self

    def add_overstocked_item(
        self,
        product_id: str = "PROD003",
        location_id: str = "LOC001",
        quantity_on_hand: int = 600,
        reorder_point: int = 50,
        max_stock_level: int = 500,
    ):
        """Add inventory item that is overstocked (above max level)."""
        self._inventory_records.append(
            {
                "product_id": product_id,
                "location_id": location_id,
                "quantity_on_hand": quantity_on_hand,
                "reorder_point": reorder_point,
                "max_stock_level": max_stock_level,
                "last_updated": "2023-01-15",
            }
        )
        return self

    def build(self):
        """Return list of inventory records."""
        return self._inventory_records.copy()

    def build_tuples(self):
        """Return inventory data as tuples for Spark DataFrame creation."""
        return [
            (
                inv["product_id"],
                inv["location_id"],
                inv["quantity_on_hand"],
                inv["reorder_point"],
                inv["max_stock_level"],
                inv["last_updated"],
            )
            for inv in self._inventory_records
        ]

    def build_schema_columns(self):
        """Return column names for Spark DataFrame schema."""
        return ["product_id", "location_id", "quantity_on_hand", "reorder_point", "max_stock_level", "last_updated"]


class ScenarioBuilder:
    """Composite builder for creating complete test scenarios."""

    @staticmethod
    def build_cross_job_scenario():
        """Build a complete scenario for testing cross-job dependencies."""
        # Create customers
        customers = (
            CustomerDataBuilder()
            .add_valid_customer("CUST001", "John", "Doe", "john.doe@example.com")
            .add_valid_customer("CUST002", "Jane", "Smith", "jane.smith@example.com")
            .build()
        )

        # Create sales transactions that reference customers
        sales = (
            SalesDataBuilder()
            .add_valid_transaction("TXN001", "CUST001", "PROD001", 2, 99.99, "2023-01-15")
            .add_valid_transaction("TXN002", "CUST002", "PROD002", 1, 199.99, "2023-01-16")
            .add_valid_transaction("TXN003", "CUST001", "PROD001", 1, 99.99, "2023-01-17")
            .build()
        )

        # Create inventory
        inventory = (
            InventoryDataBuilder()
            .add_well_stocked_item("PROD001", "LOC001", 150, 50, 500)
            .add_low_stock_item("PROD002", "LOC001", 25, 30, 200)
            .build()
        )

        return {"customers": customers, "sales": sales, "inventory": inventory}

    @staticmethod
    def build_validation_failure_scenario():
        """Build scenario with various validation failures."""
        customers = (
            CustomerDataBuilder()
            .add_valid_customer("CUST001", "John", "Doe", "john.doe@example.com")
            .add_invalid_email_customer("CUST002")
            .add_duplicate_customer("CUST001")  # Duplicate
            .build()
        )

        sales = (
            SalesDataBuilder()
            .add_valid_transaction("TXN001", "CUST001", "PROD001", 2, 29.99)
            .add_invalid_quantity_transaction("TXN002")  # Zero quantity
            .build()
        )

        return {"customers": customers, "sales": sales}

    @staticmethod
    def build_data_transformation_scenario():
        """Build scenario for testing data standardization and transformations."""
        customers = CustomerDataBuilder().add_customers_needing_normalization().build()

        return {"customers": customers}
