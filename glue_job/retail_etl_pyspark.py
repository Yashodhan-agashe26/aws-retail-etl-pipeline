import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, coalesce, regexp_replace, trim, lit
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

# Initialize Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read raw data from Glue Catalog

df = glueContext.create_dynamic_frame.from_catalog(
    database="retail_raw_db",
    table_name="raw"
).toDF()

# Force all columns to STRING to avoid Spark auto-parsing failures
for c in df.columns:
    df = df.withColumn(c, col(c).cast(StringType()))

# ----------------------------
# DATA CLEANING & TRANSFORM
# ----------------------------

# Rename columns to snake_case
df = (
    df.withColumnRenamed("Row ID", "row_id")
      .withColumnRenamed("Order ID", "order_id")
      .withColumnRenamed("Order Date", "order_date")
      .withColumnRenamed("Ship Date", "ship_date")
      .withColumnRenamed("Ship Mode", "ship_mode")
      .withColumnRenamed("Customer ID", "customer_id")
      .withColumnRenamed("Product ID", "product_id")
      .withColumnRenamed("Sub-Category", "sub_category")
      .withColumnRenamed("Product Name", "product_name")
)

# ----------------------------
# ROBUST DATE CLEANING
# ----------------------------

# standardize separators and trim spaces
df = (
    df.withColumn("order_date_clean", regexp_replace(trim(col("order_date")), "-", "/"))
      .withColumn("ship_date_clean", regexp_replace(trim(col("ship_date")), "-", "/"))
)

# try multiple date formats safely
df = (
    df.withColumn(
        "order_date",
        coalesce(
            to_date(col("order_date_clean"), "M/d/yyyy"),
            to_date(col("order_date_clean"), "d/M/yyyy"),
            to_date(col("order_date_clean"), "yyyy/M/d")
        )
    )
    .withColumn(
        "ship_date",
        coalesce(
            to_date(col("ship_date_clean"), "M/d/yyyy"),
            to_date(col("ship_date_clean"), "d/M/yyyy"),
            to_date(col("ship_date_clean"), "yyyy/M/d")
        )
    )
)

df = df.drop("order_date_clean", "ship_date_clean")

# ----------------------------
# NUMERIC CAST + NULL HANDLING
# ----------------------------

df = (
    df.withColumn("sales", col("Sales").cast("double"))
      .withColumn("quantity", col("Quantity").cast("int"))
      .withColumn("discount", col("Discount").cast("double"))
      .withColumn("profit", col("Profit").cast("double"))
)

df = df.fillna({
    "sales": 0.0,
    "quantity": 0,
    "discount": 0.0,
    "profit": 0.0
})

# ----------------------------
# FEATURE ENGINEERING
# ----------------------------

df = (
    df.withColumn("cost_estimate", col("sales") - col("profit"))
      .withColumn("discount_amount", col("sales") * col("discount"))
)

# ----------------------------
# WRITE TO S3 AS PARQUET
# ----------------------------

output_path = "path"

(
    df.write.mode("overwrite")
    .parquet(output_path)
)

job.commit()
