from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

customers_rules = {
    "rule_1" : "customer_id IS NOT NULL"
}

products_rules = {
    "rule_1" : "product_id IS NOT NULL"
}

@dp.materialized_view(name = "silver_erp_customer_details")
@dp.expect_all_or_drop(customers_rules)
def customer_details_trans():
    df = (
        spark.read.table("bronze_erp_customer_details")
        .withColumn(
            "customer_id",
            when(
                col("cid").like("NAS%"),
                substring(col("cid"), 4, length(col("cid")))
            ).otherwise(col("cid"))
        )
        .withColumn(
            "birth_date",
            when(col("bdate") > current_date(), None)
            .otherwise(col("bdate"))
        )
        .withColumn(
            "gender",
            when(upper(trim(col("gen"))).isin("F", "FEMALE"), "Female")
            .when(upper(trim(col("gen"))).isin("M", "MALE"), "Male")
            .otherwise("n/a")
        )
        .select("customer_id", "birth_date", "gender")
    )
    return df

@dp.materialized_view(name = "silver_erp_customer_location")
@dp.expect_all_or_drop(customers_rules)
def customer_location_trans():
    df = (
        spark.read.table("bronze_erp_customer_location")
        .withColumn(
            "customer_id",
            regexp_replace(col("cid"), "-", "")
        )
        .withColumn(
            "country",
            when(trim(col("cntry")) == "DE", "Germany")
            .when(trim(col("cntry")).isin("US", "USA"), "United States")
            .when((trim(col("cntry")) == "") | col("cntry").isNull(), "n/a")
            .otherwise(trim(col("cntry")))
        )
        .select("customer_id", "country")
    )
    return df


@dp.materialized_view(name = "silver_erp_product_details")
@dp.expect_all_or_drop(products_rules)
def product_details_trans():
    df = (
        spark.read.table("bronze_erp_product_details").
        select(
            col("id").alias("product_id"),
            col("cat").alias("category"),
            col("subcat").alias("sub_category"),
            col("maintenance").alias("maintainance")
        )
    )
    return df






























