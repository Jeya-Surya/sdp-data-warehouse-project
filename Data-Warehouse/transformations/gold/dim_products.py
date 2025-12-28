from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dp.view(name = "gold_dim_products")
def dim_products():
    w = Window.orderBy(col("p.start_date"), col("p.product_key"))

    df = (
        spark.read.table("silver_crm_products").alias("p")
        .filter(col("p.end_date").isNull())
        .join(
            spark.read.table("silver_erp_product_details").alias("pd"),
            col("p.category_id") == col("pd.product_id"),
            "left"
        )
        .withColumn(
            "product_surrogate_key",
            row_number().over(w)
        )
        .select(
            col("product_surrogate_key"),
            col("p.product_id"),
            col("p.product_key"),
            col("p.product_name"),
            col("p.category_id"),
            col("pd.category"),
            col("pd.sub_category"),
            col("pd.maintainance"),
            col("p.cost"),
            col("p.product_line"),
            col("p.start_date")
        )
    )
    return df