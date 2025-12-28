from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dp.view(name = "gold_dim_customers")
def dim_customers():
    w = Window.orderBy("customer_id")

    df = (
        spark.read.table("silver_crm_customers").alias("c")
        .join(
            spark.read.table("silver_erp_customer_details").alias("cd"),
            on = "customer_id",
            how = "left"
        )
        .join(
            spark.read.table("silver_erp_customer_location").alias("cl"),
            on = "customer_id",
            how = "left"
        )
        .withColumn(
            "customer_surrogate_key",
            row_number().over(w)
        )
        .withColumn(
            "customer_gender",
            when(col("c.gender") != "n/a", col("c.gender"))
            .otherwise(coalesce(col("cd.gender"), lit("n/a")))
        )
        .select(
            col("customer_surrogate_key"),
            col("c.customer_id"),
            col("c.customer_key"),
            col("c.first_name"),
            col("c.last_name"),
            col("cl.country"),
            col("c.marital_status"),
            col("customer_gender"),
            col("cd.birth_date"),
            col("c.customer_login_date")
        )
    )
    return df

