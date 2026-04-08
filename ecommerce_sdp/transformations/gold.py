# Create business logic gold layer.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

#Req 1: create gold_daily_revenue table

@dp.materialized_view(
    name = "gold.daily_revenue",
    comment = "daily revenue"
)
def daily_revenue():
    return (
        spark.read.table("silver.orders")
        .where("order_status = 'completed' or order_status = 'shipped'")
        .groupBy("order_date", "shipping_city", "payment_method")
        .agg(
            F.sum(F.col("unit_price") * F.col("quantity")).alias("Total_Revenue"),
            F.count(F.col("order_id")).alias("order_count"),
            F.sum("quantity").alias("total_items_sold")
        )
        .withColumn("average_order_value", F.col("Total_Revenue") / F.col("order_count"))
        .orderBy("order_date")
                                                                                                                    
    )

@dp.materialized_view(
    name = "gold.customer_360",
    comment = "daily revenue"
)
def customer_360():
    #Since we are doing aggregrations.  First we should aggregrate, then
        #Join

    order_metrics = (
        spark.read.table("silver.orders")
        .groupBy("customer_id")
        .agg(
          F.sum("total_amount").alias("Lifetime_Value"),
          F.count("order_id").alias("Total_Customer_Orders"),
          F.datediff(F.current_date(), F.max("order_date")).alias("days_since_last_order")
        ).orderBy("customer_id")
        )

        #  Now perform the join with customers
    return (
        order_metrics.join(spark.read.table("silver.customers_stream"), on = "customer_id", how = "left")
        .withColumn("Customer_Segmentation", 
            F.when(F.col("Lifetime_Value") > 5000, "High Value Customer")
            .when(F.col("Lifetime_Value") > 1000, "Medium Value Customer")
            .when(F.col("Lifetime_Value") > 0, "Low Value Customer")
            .otherwise("No Purchases"))
        .select("customer_id", "days_since_last_order", "Total_Customer_Orders", "Lifetime_Value", "Customer_Segmentation")
        )

    
@dp.materialized_view(
        name="gold.top_products",
        comment="this table represents the top products of the company"
    )
def top_products():
    filtered_orders = spark.read.table("silver.orders").where("order_status = 'completed' or order_status = 'shipped'")
        
    return ( 
            filtered_orders.groupBy("product_id")
            .agg( F.sum(col("total_amount") * col("quantity")).alias("total_revenue") )
                
        )





