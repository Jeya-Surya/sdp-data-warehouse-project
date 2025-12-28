from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dp.materialized_view(name = "gold_fact_sales")
def fact_sales():
        
    df = (
        spark.read.table("silver_crm_sales").alias("sd")
        .join(
            spark.read.table("gold_dim_products").alias("pr"),
            col("sd.product_key") == col("pr.product_key"),
            "left"
        )
        .join(
            spark.read.table("gold_dim_customers").alias("cu"),
            col("sd.customer_id") == col("cu.customer_id"),
            "left"
        )
        .select(
            col("sd.order_id").alias("order_number"),
            col("pr.product_surrogate_key").alias("product_key"),
            col("cu.customer_surrogate_key").alias("customer_key"),
            col("sd.order_date"),
            col("sd.shipment_date"),
            col("sd.due_date"),
            col("sd.sales").alias("sales_amount"),
            col("sd.quantity"),
            col("sd.price")
        )
    )
    return df