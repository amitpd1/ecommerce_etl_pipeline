# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC ### This will generate dummy data
# MAGIC
# MAGIC This will generate dummy data
# MAGIC

# COMMAND ----------

from datetime import datetime, timedelta
import random

volume_path = "/Volumes/ecommerce/bronze/raw_files"
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

##Git Checkin


# COMMAND ----------

from datetime import datetime, timedelta
import random

# 3. Generate Customers (500 rows)
volume_path = "/Volumes/ecommerce/bronze/raw_files"
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


# COMMAND ----------

from datetime import datetime, timedelta
import random

# 3. Generate Customers (500 rows)
volume_path = "/Volumes/ecommerce/bronze/raw_files"

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

