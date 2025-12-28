from pyspark import pipelines as dp

@dp.materialized_view(name = "bronze_erp_customer_details")
def customer_details_ingestion():
    df = spark.read.csv("/Volumes/data-warehouse/sources/source_erp/CUST_AZ12.csv", header = "true", inferSchema = "true")
    return df

@dp.materialized_view(name = "bronze_erp_customer_location")
def customer_location_ingestion():
    df = spark.read.csv("/Volumes/data-warehouse/sources/source_erp/LOC_A101.csv", header = "true", inferSchema = "true")
    return df

@dp.materialized_view(name = "bronze_erp_product_details")
def product_details_ingestion():
    df = spark.read.csv("/Volumes/data-warehouse/sources/source_erp/PX_CAT_G1V2.csv", header = "true", inferSchema = "true")
    return df