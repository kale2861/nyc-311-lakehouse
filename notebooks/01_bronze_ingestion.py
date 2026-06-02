# Databricks notebook source
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/raw/311_service_requests.csv")
)
display(df.limit(10))

# COMMAND ----------

import re

file_path = "/Volumes/workspace/default/raw/311_service_requests.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(file_path)
)

def clean_column_name(col_name):
    col_name = col_name.lower()
    col_name = re.sub(r"[^a-z0-9]+", "_", col_name)
    col_name = col_name.strip("_")
    return col_name

clean_columns = [
    clean_column_name(c)
    for c in df.columns
]

df_clean = df.toDF(*clean_columns)

display(df_clean.limit(10))

# COMMAND ----------

df_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_311_requests")

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM bronze_311_requests
        LIMIT 10
    """)
)

# COMMAND ----------

spark.table("bronze_311_requests").count()

# COMMAND ----------

spark.table("bronze_311_requests").printSchema()

# COMMAND ----------

display(
    spark.table("bronze_311_requests")
    .select(
        "unique_key",
        "created_date",
        "agency",
        "borough",
        "status"
    )
    .limit(20)
)