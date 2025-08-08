#!/usr/bin/env python3
"""
Script to generate large test datasets for performance testing.
Run this script to create large CSV files for testing job performance.
"""

import csv
import random
import string
from datetime import datetime, timedelta
from pathlib import Path


def generate_customers(num_records=10000):
    """Generate large customer dataset"""

    first_names = ["John", "Jane", "Bob", "Alice", "Charlie", "Emma", "David", "Sarah", "Mike", "Lisa"]
    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
    ]
    domains = ["example.com", "test.com", "demo.org", "sample.net", "company.com"]

    output_file = Path(__file__).parent / "customers_large.csv"

    with open(output_file, "w", newline="") as csvfile:
        fieldnames = ["customer_id", "first_name", "last_name", "email", "phone", "registration_date"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        base_date = datetime(2023, 1, 1)

        for i in range(num_records):
            customer_id = f"CUST{i + 1:06d}"
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)

            # Introduce some data quality issues
            if i % 500 == 0:  # 0.2% invalid emails
                email = f"{first_name.lower()}.{last_name.lower()}@invalid"
            elif i % 1000 == 0:  # 0.1% empty emails
                email = ""
            else:
                email = f"{first_name.lower()}.{last_name.lower()}{i}@{random.choice(domains)}"

            phone = f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            reg_date = base_date + timedelta(days=random.randint(0, 365))

            writer.writerow(
                {
                    "customer_id": customer_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                    "phone": phone,
                    "registration_date": reg_date.strftime("%Y-%m-%d"),
                }
            )

    print(f"Generated {num_records} customers in {output_file}")


def generate_sales(num_records=50000, num_customers=10000, num_products=1000):
    """Generate large sales dataset"""

    output_file = Path(__file__).parent / "sales_large.csv"

    with open(output_file, "w", newline="") as csvfile:
        fieldnames = [
            "transaction_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "total_amount",
            "transaction_date",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        base_date = datetime(2023, 1, 1)

        for i in range(num_records):
            transaction_id = f"TXN{i + 1:08d}"
            customer_id = f"CUST{random.randint(1, num_customers):06d}"
            product_id = f"PROD{random.randint(1, num_products):03d}"

            # Introduce some data quality issues
            if i % 5000 == 0:  # 0.02% zero quantities
                quantity = 0
            elif i % 10000 == 0:  # 0.01% negative quantities
                quantity = -random.randint(1, 5)
            else:
                quantity = random.randint(1, 10)

            unit_price = round(random.uniform(9.99, 999.99), 2)

            # Introduce price inconsistencies
            if i % 2000 == 0:  # 0.05% price mismatches
                total_amount = round(random.uniform(10.0, 1000.0), 2)
            else:
                total_amount = round(quantity * unit_price, 2)

            trans_date = base_date + timedelta(days=random.randint(0, 365))

            writer.writerow(
                {
                    "transaction_id": transaction_id,
                    "customer_id": customer_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_amount": total_amount,
                    "transaction_date": trans_date.strftime("%Y-%m-%d"),
                }
            )

    print(f"Generated {num_records} sales records in {output_file}")


def generate_products(num_records=1000):
    """Generate large product dataset"""

    categories = [
        "Electronics > Phones",
        "Electronics > Computers",
        "Electronics > Audio",
        "Clothing > Men's",
        "Clothing > Women's",
        "Clothing > Children's",
        "Home > Kitchen",
        "Home > Furniture",
        "Home > Decor",
        "Sports > Outdoor",
        "Sports > Fitness",
        "Sports > Team Sports",
        "Books > Fiction",
        "Books > Non-Fiction",
        "Books > Educational",
    ]

    brands = ["BrandA", "BrandB", "BrandC", "TechCorp", "QualityMaker", "ValueBrand", "PremiumTech", "EcoFriendly"]

    output_file = Path(__file__).parent / "products_large.csv"

    with open(output_file, "w", newline="") as csvfile:
        fieldnames = ["product_id", "product_name", "category", "brand", "price", "cost", "sku", "in_stock"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for i in range(num_records):
            product_id = f"PROD{i + 1:03d}"

            # Generate product names with some variation
            base_names = ["Widget", "Gadget", "Device", "Tool", "Accessory", "Component", "System", "Kit"]
            product_name = f"{random.choice(base_names)} {i + 1}"

            category = random.choice(categories)
            brand = random.choice(brands)

            # Price distribution with some outliers
            if i % 100 == 0:  # 1% premium products
                price = round(random.uniform(500.0, 2000.0), 2)
            elif i % 50 == 0:  # 2% budget products
                price = round(random.uniform(1.0, 20.0), 2)
            else:
                price = round(random.uniform(20.0, 500.0), 2)

            # Cost calculation with some margin variation
            margin = random.uniform(0.3, 0.7)  # 30-70% margin
            cost = round(price * (1 - margin), 2)

            # Introduce some data quality issues
            if i % 200 == 0:  # 0.5% negative prices
                price = -price
            elif i % 300 == 0:  # 0.33% zero prices
                price = 0.0

            sku = f"SKU-{i + 1:04d}"

            # Introduce duplicate SKUs occasionally
            if i % 150 == 0 and i > 0:  # 0.67% duplicate SKUs
                sku = f"SKU-{(i // 2) + 1:04d}"

            in_stock = random.choice([True, False]) if i % 10 != 0 else True  # 90% in stock

            writer.writerow(
                {
                    "product_id": product_id,
                    "product_name": product_name,
                    "category": category,
                    "brand": brand,
                    "price": price,
                    "cost": cost,
                    "sku": sku,
                    "in_stock": in_stock,
                }
            )

    print(f"Generated {num_records} products in {output_file}")


def generate_malformed_data():
    """Generate intentionally malformed data for error handling tests"""

    output_file = Path(__file__).parent / "malformed_data.csv"

    with open(output_file, "w", newline="") as csvfile:
        # Intentionally write malformed CSV
        csvfile.write("customer_id,name,email,phone\n")
        csvfile.write('CUST001,"John Doe",john@example.com,555-1234\n')
        csvfile.write('CUST002,Jane "Quotes" Smith,jane@example.com,555-5678\n')  # Unescaped quotes
        csvfile.write("CUST003,Bob,bob@example.com,555-9999\n")
        csvfile.write('CUST004,"Incomplete row\n')  # Missing closing quote and fields
        csvfile.write("CUST005,Alice,alice@example.com,555-1111,extra,fields,here\n")  # Too many fields
        csvfile.write("CUST006\n")  # Missing fields
        csvfile.write('"CUST007","Name with\nnewline",email@test.com,555-2222\n')  # Newline in field
        csvfile.write('CUST008,"Name with\ttab",tab@test.com,555-3333\n')  # Tab in field

    print(f"Generated malformed data in {output_file}")


def generate_unicode_test_data():
    """Generate data with various Unicode characters"""

    output_file = Path(__file__).parent / "unicode_test_data.csv"

    unicode_data = [
        ("CUST001", "José", "García", "jose@example.com"),
        ("CUST002", "François", "Müller", "francois@test.fr"),
        ("CUST003", "李", "小明", "li@example.cn"),
        ("CUST004", "محمد", "العربي", "mohammed@example.ae"),
        ("CUST005", "Владимир", "Петров", "vladimir@example.ru"),
        ("CUST006", "Γιάννης", "Παπαδόπουλος", "yannis@example.gr"),
        ("CUST007", "🎉", "Emoji", "emoji@test.com"),
        ("CUST008", "Mixed 中文 English", "Test", "mixed@example.com"),
    ]

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["customer_id", "first_name", "last_name", "email"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for customer_id, first_name, last_name, email in unicode_data:
            writer.writerow(
                {"customer_id": customer_id, "first_name": first_name, "last_name": last_name, "email": email}
            )

    print(f"Generated Unicode test data in {output_file}")


if __name__ == "__main__":
    print("Generating large test datasets...")

    # Generate different sized datasets
    generate_customers(10000)  # 10K customers
    generate_sales(50000, 10000, 1000)  # 50K sales, referencing customers and products
    generate_products(1000)  # 1K products

    # Generate edge case datasets
    generate_malformed_data()
    generate_unicode_test_data()

    print("Done! Large test datasets generated.")
    print("\nTo generate even larger datasets, modify the record counts in the script.")
    print("Warning: Very large datasets (>100K records) may take significant time to process in tests.")
