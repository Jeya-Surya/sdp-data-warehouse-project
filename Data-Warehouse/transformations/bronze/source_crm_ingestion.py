from pyspark import pipelines as dp

@dp.materialized_view(name = "bronze_crm_customers")
def customers_ingestion():
    df = spark.read.csv("/Volumes/data-warehouse/sources/source_crm/cust_info.csv", header = "true", inferSchema = "true")
    return df

@dp.materialized_view(name = "bronze_crm_products")
def products_ingestion():
    df = spark.read.csv("/Volumes/data-warehouse/sources/source_crm/prd_info.csv", header = "true", inferSchema = "true")
    return df

@dp.materialized_view(name = "bronze_crm_sales")
def sales_ingestion():
    df = spark.read.csv("/Volumes/data-warehouse/sources/source_crm/sales_details.csv", header = "true", inferSchema = "true")
    return df
