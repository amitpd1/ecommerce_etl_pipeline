#  The silver layer is where we transform the data
from pyspark import pipelines as dp
from pyspark.sql import functions as F # Added this for F.col and F.lit
from pyspark.sql.functions import col, to_date, year, month, upper, initcap

@dp.materialized_view(
    name="silver.orders",
    comment="Cleaned Orders data"
)
@dp.expect("order_id_is_not_null", "order_id IS NOT NULL")
@dp.expect("quantity_greater_than_zero", "quantity > 0")
@dp.expect("unit_price_greater_than_zero", "unit_price > 0")
@dp.expect("order_date_is_not_null", "order_date IS NOT NULL")
def silver_orders_cleaned():
    return (spark.read.table("ecommerce.bronze.orders_raw")
            .withColumn("total_amount", col("quantity") * col("unit_price"))
            .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
            .withColumn("order_year", year(col("order_date")))
            .withColumn("order_month", month(col("order_date")))
            .withColumn("quantity", col("quantity").cast("int"))
            )
#
@dp.materialized_view(
    name="silver.customers",
    comment="Cleaned Customers data, with expectation"
)
@dp.expect("customer_id_is_not_null", "customer_id IS NOT NULL")
@dp.expect("email has @", "email like '%@%'")
def silver_customers_cleaned():
    return (
            spark.read.table("ecommerce.bronze.customers_raw")
            .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
            .withColumn("tier", upper(col("tier")))
            .withColumn("city", initcap(col("city")))
            # To match the metadata
            .withColumn("cdc_timestamp", F.col("signup_date").cast("string")) 
            .withColumn("operation", F.lit("INITIAL"))
            .withColumn("is_active", F.lit("true"))
            )
    
@dp.temporary_view(name="customer_cdc")
def prepare_cdc():
    return spark.readStream.table("ecommerce.bronze.customer_cdc_raw") \
        .withColumn("signup_date", F.lit(None).cast("date"))    


# now we need to apply CDC
# Auto CDC will create and manage the silver.customers_stream table automatically

# Step 1: Create target streaming table
dp.create_streaming_table(
    name = "silver.customers_stream",
    comment = "CDC table for customers with SCD type 2"
)

# Step 2: INITIAL LOAD from snapshot
dp.create_auto_cdc_flow(
    target = "silver.customers_stream",
    source = "silver.customers",
    keys = ["customer_id"],
    sequence_by = "cdc_timestamp",
    apply_as_deletes = "operation = 'DELETE'", 
    stored_as_scd_type = 2,
    name = "initial_hydration_flow",
    once = True # This tells LDP to process the batch once and stop
)

# Step 3: Apply incremental CDC
dp.create_auto_cdc_flow(
    target = "silver.customers_stream",
    source = "customer_cdc",
    keys = ["customer_id"],
    sequence_by = "cdc_timestamp",
    apply_as_deletes = "operation = 'DELETE'", 
    stored_as_scd_type = 2,
    name = "incremental_customer_updates",
    once = False
)
