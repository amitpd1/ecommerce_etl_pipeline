# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC ### Thi ill generate dummy data
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

