"""
Test data factories using factory-boy for clean and maintainable test data generation.

These factories replace the custom test data builders and provide a more declarative
approach to creating test data with realistic business scenarios and edge cases.
"""

import factory
from datetime import datetime, date
from typing import Any, Dict


class CustomerFactory(factory.Factory):
    """Factory for creating customer test data with realistic defaults."""
    
    class Meta:
        model = dict
    
    customer_id = factory.Sequence(lambda n: f"CUST{n:03d}")
    first_name = factory.Faker("first_name")
    last_name = factory.Faker("last_name") 
    email = factory.LazyAttribute(lambda obj: f"{obj.first_name.lower()}.{obj.last_name.lower()}@example.com")
    phone = factory.Faker("phone_number")
    registration_date = factory.Faker("date_between", start_date="-2y", end_date="today")
    
    @classmethod
    def with_invalid_email(cls, **kwargs):
        """Create customer with invalid email for validation testing."""
        return cls(email="not-an-email", **kwargs)
    
    @classmethod
    def needing_normalization(cls, **kwargs):
        """Create customer with data that needs standardization."""
        return cls(
            first_name="  alice  ",  # Extra whitespace
            last_name="  WILLIAMS  ",  # Mixed case
            email="ALICE.WILLIAMS@EXAMPLE.COM",  # Uppercase email
            phone="555.123.4567",  # Dots in phone
            **kwargs
        )


class SalesTransactionFactory(factory.Factory):
    """Factory for creating sales transaction test data."""
    
    class Meta:
        model = dict
    
    transaction_id = factory.Sequence(lambda n: f"TXN{n:03d}")
    customer_id = factory.Sequence(lambda n: f"CUST{n:03d}")
    product_id = factory.Sequence(lambda n: f"PROD{n:03d}")
    quantity = factory.Faker("random_int", min=1, max=10)
    unit_price = factory.Faker("pydecimal", left_digits=3, right_digits=2, positive=True)
    transaction_date = factory.Faker("date_between", start_date="-1y", end_date="today")
    
    # Calculate total_amount based on quantity and unit_price
    total_amount = factory.LazyAttribute(lambda obj: float(obj.quantity) * float(obj.unit_price))
    
    @classmethod
    def with_invalid_quantity(cls, **kwargs):
        """Create transaction with zero/negative quantity for validation testing."""
        return cls(quantity=0, total_amount=0, **kwargs)
    
    @classmethod
    def for_customer(cls, customer_id: str, **kwargs):
        """Create transaction for specific customer."""
        return cls(customer_id=customer_id, **kwargs)


class InventoryFactory(factory.Factory):
    """Factory for creating inventory test data."""
    
    class Meta:
        model = dict
    
    product_id = factory.Sequence(lambda n: f"PROD{n:03d}")
    location_id = factory.Sequence(lambda n: f"LOC{n:03d}")
    quantity_on_hand = factory.Faker("random_int", min=50, max=500)
    reorder_point = factory.Faker("random_int", min=20, max=100)
    max_stock_level = factory.Faker("random_int", min=200, max=1000)
    last_updated = factory.Faker("date_between", start_date="-30d", end_date="today")
    
    @classmethod
    def low_stock(cls, **kwargs):
        """Create inventory item below reorder point."""
        return cls(
            quantity_on_hand=25,
            reorder_point=30,
            max_stock_level=200,
            **kwargs
        )
    
    @classmethod
    def well_stocked(cls, **kwargs):
        """Create inventory item well above reorder point."""
        return cls(
            quantity_on_hand=150,
            reorder_point=50, 
            max_stock_level=500,
            **kwargs
        )
    
    @classmethod
    def overstocked(cls, **kwargs):
        """Create inventory item above max stock level."""
        return cls(
            quantity_on_hand=600,
            reorder_point=50,
            max_stock_level=500,
            **kwargs
        )


class SparkDataFrameFactory:
    """Utility factory for creating PySpark DataFrames from factory data."""
    
    @staticmethod
    def create_customer_df(spark, count=5, **factory_kwargs):
        """Create customer DataFrame using CustomerFactory."""
        customers = CustomerFactory.build_batch(count, **factory_kwargs)
        schema = ["customer_id", "first_name", "last_name", "email", "phone", "registration_date"]
        data = [(c["customer_id"], c["first_name"], c["last_name"], c["email"], c["phone"], str(c["registration_date"])) 
                for c in customers]
        return spark.createDataFrame(data, schema)
    
    @staticmethod
    def create_sales_df(spark, count=5, **factory_kwargs):
        """Create sales DataFrame using SalesTransactionFactory."""
        transactions = SalesTransactionFactory.build_batch(count, **factory_kwargs)
        schema = ["transaction_id", "customer_id", "product_id", "quantity", "unit_price", "total_amount", "transaction_date"]
        data = [(t["transaction_id"], t["customer_id"], t["product_id"], t["quantity"], 
                float(t["unit_price"]), t["total_amount"], str(t["transaction_date"]))
                for t in transactions]
        return spark.createDataFrame(data, schema)
    
    @staticmethod
    def create_inventory_df(spark, count=5, **factory_kwargs):
        """Create inventory DataFrame using InventoryFactory."""
        inventory = InventoryFactory.build_batch(count, **factory_kwargs)
        schema = ["product_id", "location_id", "quantity_on_hand", "reorder_point", "max_stock_level", "last_updated"]
        data = [(i["product_id"], i["location_id"], i["quantity_on_hand"], 
                i["reorder_point"], i["max_stock_level"], str(i["last_updated"]))
                for i in inventory]
        return spark.createDataFrame(data, schema)


# Scenario builders for complex test cases
class ScenarioFactory:
    """Factory for creating complete test scenarios with related data."""
    
    @staticmethod
    def cross_job_scenario():
        """Build complete scenario for testing cross-job dependencies."""
        customers = CustomerFactory.build_batch(2)
        
        # Create sales for the customers
        sales = [
            SalesTransactionFactory.for_customer(customers[0]["customer_id"], product_id="PROD001"),
            SalesTransactionFactory.for_customer(customers[1]["customer_id"], product_id="PROD002"),
            SalesTransactionFactory.for_customer(customers[0]["customer_id"], product_id="PROD001"),
        ]
        
        # Create inventory for the products
        inventory = [
            InventoryFactory.well_stocked(product_id="PROD001"),
            InventoryFactory.low_stock(product_id="PROD002"),
        ]
        
        return {
            "customers": customers,
            "sales": sales, 
            "inventory": inventory
        }
    
    @staticmethod
    def validation_failure_scenario():
        """Build scenario with various validation failures."""
        customers = [
            CustomerFactory(),  # Valid customer
            CustomerFactory.with_invalid_email(),  # Invalid email
            CustomerFactory(),  # Will be duplicated
        ]
        
        # Add duplicate (same customer_id)
        duplicate = customers[0].copy()
        customers.append(duplicate)
        
        sales = [
            SalesTransactionFactory.for_customer(customers[0]["customer_id"]),  # Valid
            SalesTransactionFactory.with_invalid_quantity(),  # Invalid quantity
        ]
        
        return {
            "customers": customers,
            "sales": sales
        }