This is a weekend Ecommerce project, where we build an ETL pipeline with Medallion arcetucture with Spark Declarative Pipelines and Databricks.

1. Project Overview
The project implements a declarative ETL pipeline on Databricks using Lakeflow. It processes three main data streams:
1.	Orders: Transactional data from an e-commerce platform.
2.	Customers: Profile information for users.
3.	Customer CDC: A stream of changes (inserts, updates, deletes) from a source database to track customer history.
________________________________________
2. Environment Setup
•	Catalog Name: learn_lakeflow
•	Schema Name: ecommerce
•	Storage: A Databricks Volume named raw_files located at /Volumes/learn_lakeflow/ecommerce/raw_files.
•	Compute: Databricks Serverless or any Spark-enabled cluster.
________________________________________
3. Data Generation (Source Data)
To simulate a real-world scenario, you must first generate CSV files in the raw_files volume.
Script: Generate Initial Source Data
Run this script in a standard Databricks notebook to create the initial dataset.
Python
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random

# Configuration
CATALOG = "learn_lakeflow"
SCHEMA = "ecommerce"
volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"

# 1. Create Volume
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_files")

# 2. Generate Orders (5,000 rows)
order_data = []
for i in range(1, 5001):
    order_data.append({
        "order_id": f"ORD-{i:06d}",
        "customer_id": f"CUST-{random.randint(1, 500):04d}",
        "product_id": f"PROD-{random.randint(1, 50):03d}",
        "quantity": random.randint(1, 10),
        "unit_price": round(random.uniform(9.99, 299.99), 2),
        "order_status": random.choice(["completed", "pending", "shipped", "cancelled", "returned"]),
        "order_date": (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
        "payment_method": random.choice(["credit_card", "debit_card", "upi", "wallet", "cod"]),
        "shipping_city": random.choice(["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad", "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow"])
    })
orders_df = spark.createDataFrame(order_data)
orders_df.write.mode("overwrite").option("header", "true").csv(f"{volume_path}/orders/")

# 3. Generate Customers (500 rows)
customer_data = []
for i in range(1, 501):
    customer_data.append({
        "customer_id": f"CUST-{i:04d}",
        "customer_name": f"Customer_{i}",
        "email": f"customer{i}@example.com",
        "signup_date": (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))).strftime("%Y-%m-%d"),
        "tier": random.choice(["bronze", "silver", "gold", "platinum"]),
        "city": random.choice(["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad"]),
        "is_active": random.choice([True, True, True, False])
    })
customers_df = spark.createDataFrame(customer_data)
customers_df.write.mode("overwrite").option("header", "true").csv(f"{volume_path}/customers/")

# 4. Generate CDC Events (200 events)
cdc_events = []
for i in range(1, 201):
    cust_id = f"CUST-{random.randint(1, 500):04d}"
    op = random.choice(["INSERT", "UPDATE", "UPDATE", "DELETE"])
    cdc_events.append({
        "cdc_timestamp": (datetime(2024, 6, 1) + timedelta(hours=random.randint(0, 4000))).strftime("%Y-%m-%d %H:%M:%S"),
        "operation": op,
        "customer_id": cust_id,
        "customer_name": f"Customer_{cust_id.split('-')[1]}_v2" if op == "UPDATE" else f"Customer_{cust_id.split('-')[1]}",
        "email": f"updated_{cust_id.split('-')[1]}@example.com" if op == "UPDATE" else f"customer{cust_id.split('-')[1]}@example.com",
        "tier": random.choice(["bronze", "silver", "gold", "platinum"]),
        "city": random.choice(["Mumbai", "Delhi", "Bangalore", "Chennai"]),
        "is_active": op != "DELETE"
    })
cdc_df = spark.createDataFrame(cdc_events)
cdc_df.write.mode("overwrite").option("header", "true").csv(f"{volume_path}/customer_cdc/")
________________________________________
4. Pipeline Phase 1: Bronze Layer (Raw Ingestion)
Goal: Ingest data from the Volume into Delta tables with minimal transformation.
•	Technology: Use Auto Loader (cloudFiles) for incremental ingestion.
•	Format: Source is CSV; Destination is Streaming Tables.
•	Schema Evolution: Enable schema inference.
•	Tables to Create:
1.	bronze_orders: Ingest from .../raw_files/orders/.
2.	bronze_customers: Ingest from .../raw_files/customers/.
3.	bronze_customer_cdc: Ingest from .../raw_files/customer_cdc/.
________________________________________
5. Pipeline Phase 2: Silver Layer (Cleaning & Enrichment)
Goal: Transform raw data into cleaned, validated, and joined datasets.
Table: silver_orders (Materialized View)
•	Logic:
•	Calculate total_amount = quantity * unit_price.
•	Convert order_date string to Date type.
•	Extract order_year and order_month.
•	Data Quality Rules (Expectations):
1.	order_id must not be NULL.
2.	quantity must be greater than 0 (drop record if violated).
3.	unit_price must be greater than 0 (drop record if violated).
4.	order_date must not be NULL.
Table: silver_customers (Materialized View)
•	Logic:
•	Convert signup_date string to Date type.
•	Standardize tier to Uppercase.
•	Standardize city to Initial Capitalization (e.g., "mumbai" -> "Mumbai").
•	Data Quality Rules:
1.	customer_id must not be NULL (fail pipeline if violated).
2.	email must contain '@'.
Table: silver_customer_dim_scd2 (SCD Type 2)
•	Logic: Implement Slowly Changing Dimension Type 2 for customer profile changes.
•	Mechanism: Use apply_changes.: create_auto_cdc_flow()
•	Keys: customer_id.
•	Sequence By: cdc_timestamp.
•	Deletes: Where operation = 'DELETE'.
•	History: Track full history (SCD Type 2).
________________________________________
6. Pipeline Phase 3: Gold Layer (Business Analytics)
Goal: Create high-level aggregations for reporting.
Table: gold_daily_revenue
•	Source: silver_orders.
•	Filters: Only include orders where status is "completed" or "shipped".
•	Aggregation: Group by order_date, shipping_city, and payment_method.
•	Metrics: Total revenue, order count, average order value, total items sold.
Table: gold_customer_360
•	Source: Join silver_customers with aggregated metrics from silver_orders.
•	Logic:
•	Calculate Lifetime Value (LTV) and total orders per customer.
•	Calculate days_since_last_order.
•	Customer Segmentation:
•	high_value: LTV > 5000
•	medium_value: LTV > 1000
•	low_value: LTV > 0
•	no_purchases: Otherwise.
Table: gold_top_products
•	Source: silver_orders (filtered for completed/shipped).
•	Aggregation: Group by product_id.
•	Metrics: Total revenue, total quantity, unique buyers.
•	Feature: Rank products by revenue using a window function.
________________________________________
7. Incremental Testing Script
To verify that the pipeline only processes new data (incremental loading), use this script to append new records after the first run.
Python
# Generate 500 new orders for a new date
new_orders = []
for i in range(5001, 5501):
    new_orders.append({
        "order_id": f"ORD-{i:06d}",
        "customer_id": f"CUST-{random.randint(1, 500):04d}",
        "product_id": f"PROD-{random.randint(1, 50):03d}",
        "quantity": random.randint(1, 10),
        "unit_price": round(random.uniform(9.99, 299.99), 2),
        "order_status": "completed",
        "order_date": "2025-01-15",
        "payment_method": "upi",
        "shipping_city": "Mumbai"
    })
new_orders_df = spark.createDataFrame(new_orders)
new_orders_df.write.mode("append").option("header", "true").csv(f"{volume_path}/orders/")
print("Appended 500 new orders. Re-run pipeline to test incremental logic.")
________________________________________
8. Validation Queries
After running the pipeline, use these SQL queries to verify the results:
1.	SCD Type 2 Check: SELECT * FROM learn_lakeflow.ecommerce.silver_customer_dim_scd2 WHERE customer_id = 'CUST-0001' ORDER BY __START_AT;
2.	Top Revenue: SELECT * FROM learn_lakeflow.ecommerce.gold_daily_revenue ORDER BY daily_revenue DESC LIMIT 10;
3.	Segments: SELECT customer_segment, count(*) FROM learn_lakeflow.ecommerce.gold_customer_360 GROUP BY 1;

