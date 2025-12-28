from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

customers_rules = {
    "rule_1" : "customer_id IS NOT NULL"
}

@dp.materialized_view(name = "silver_crm_customers")
@dp.expect_all_or_drop(customers_rules)
def customers_trans():
    w = Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())

    df = (
        spark.read.table("bronze_crm_customers")
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .withColumn("customer_id", col("cst_id"))
        .withColumn("customer_key", col("cst_key"))
        .withColumn("first_name", trim(col("cst_firstname")))
        .withColumn("last_name", trim(col("cst_lastname")))
        .withColumn(
            "marital_status",
            when(upper(trim(col("cst_marital_status"))) == "S", "Single")
            .when(upper(trim(col("cst_marital_status"))) == "M", "Married")
            .otherwise("n/a")
        )
        .withColumn(
            "gender",
            when(upper(trim(col("cst_gndr"))) == "M", "Male")
            .when(upper(trim(col("cst_gndr"))) == "F", "Female")
            .otherwise("n/a")
        )
        .select(
            "customer_id",
            "customer_key",
            "first_name",
            "last_name",
            "marital_status",
            "gender",
            col("cst_create_date").alias("customer_login_date")
        )
    )
    return df


products_rules = {
    "rule_1" : "product_id IS NOT NULL"
}

@dp.materialized_view(name = "silver_crm_products")
@dp.expect_all_or_drop(products_rules)
def products_trans():
    w = Window.partitionBy("prd_key").orderBy("prd_start_dt")

    df = (
        spark.read.table("bronze_crm_products")
        .withColumn("product_id", col("prd_id"))
        .withColumn(
            "category_id",
            regexp_replace(substring(col("prd_key"), 1, 5), "-", "_")
        )
        .withColumn(
            "product_key",
            substring(col("prd_key"), 7, length(col("prd_key")))
        )
        .withColumn("product_name", col("prd_nm"))
        .withColumn("cost", coalesce(col("prd_cost"), lit(0)))
        .withColumn(
            "product_line",
            when(upper(trim(col("prd_line"))) == "M", "Mountain")
            .when(upper(trim(col("prd_line"))) == "R", "Road")
            .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
            .when(upper(trim(col("prd_line"))) == "T", "Touring")
            .otherwise("n/a")
        )
        .withColumn("start_date", col("prd_start_dt").cast("date"))
        .withColumn(
            "end_date",
            date_sub(
                lead(col("prd_start_dt")).over(w),
                1
            )
        )
        .select(
            "product_id",
            "category_id",
            "product_key",
            "product_name",
            "cost",
            "product_line",
            "start_date",
            "end_date"
        )
    )
    return df

sales_rules = {
    "rule_1" : "order_id IS NOT NULL"
}

@dp.materialized_view(name = "silver_crm_sales")
@dp.expect_all_or_drop(sales_rules)
def sales_trans():
    df = (
        spark.read.table("bronze_crm_sales")
        .withColumn(
            "order_date",
            when(
                (col("sls_order_dt") == 0) | (length(col("sls_order_dt")) != 8),
                None
            ).otherwise(
                to_date(col("sls_order_dt").cast("string"), "yyyyMMdd")
            )
        )
        .withColumn(
            "shipment_date",
            when(
                (col("sls_ship_dt") == 0) | (length(col("sls_ship_dt")) != 8),
                None
            ).otherwise(
                to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd")
            )
        )
        .withColumn(
            "due_date",
            when(
                (col("sls_due_dt") == 0) | (length(col("sls_due_dt")) != 8),
                None
            ).otherwise(
                to_date(col("sls_due_dt").cast("string"), "yyyyMMdd")
            )
        )
        .withColumn(
            "sales",
            when(
                (col("sls_sales").isNull()) |
                (col("sls_sales") <= 0) |
                (col("sls_sales") != col("sls_quantity") * abs(col("sls_price"))),
                col("sls_quantity") * abs(col("sls_price"))
            ).otherwise(col("sls_sales"))
        )
        .withColumn(
            "price",
            when(
                (col("sls_price").isNull()) | (col("sls_price") <= 0),
                col("sls_sales") / nullif(col("sls_quantity"), lit(0))
            ).otherwise(col("sls_price"))
        )
        .select(
            col("sls_ord_num").alias("order_id"),
            col("sls_prd_key").alias("product_key"),
            col("sls_cust_id").alias("customer_id"),
            "order_date",
            "shipment_date",
            "due_date",
            "sales",
            col("sls_quantity").alias("quantity"),
            "price"
        )
    )
    return df





