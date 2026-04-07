#First step is to ingest the data from the volumes into the SDP
#  for this we can use autoloader technology
from pyspark import pipelines as dp

customer_data_path = "/Volumes/ecommerce/bronze/raw_files/customers/"
orders_data_path = "/Volumes/ecommerce/bronze/raw_files/orders/"
customer_cdc_data_path = "/Volumes/ecommerce/bronze/raw_files/customer_cdc/"


@dp.table(comment="Raw data of customers",
          name="bronze.customers_raw")
def read_data_customers():
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferSchema", "true")
        .load(customer_data_path))


@dp.table(comment="Raw data of orders",
          name="bronze.orders_raw")
def read_data_orders():
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferSchema", "true")
        .load(orders_data_path))
    

@dp.table(comment="Raw data of customers cdc",
          name="bronze.customer_cdc_raw")
def read_data_customers_cdc():
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferSchema", "true")
        .load(customer_cdc_data_path))
