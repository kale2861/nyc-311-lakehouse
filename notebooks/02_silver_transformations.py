# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer
# MAGIC
# MAGIC The Silver layer standardizes data types, removes duplicates,
# MAGIC handles missing values, and derives business-ready fields.
# MAGIC
# MAGIC Transformations:
# MAGIC - Standardized timestamps
# MAGIC - Response time calculation
# MAGIC - Duplicate removal
# MAGIC - Borough normalization

# COMMAND ----------

bronze_df = spark.table("bronze_311_requests")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert Dates

# COMMAND ----------

from pyspark.sql.functions import *

silver_df = (
    bronze_df
    .withColumn(
        "created_date",
        to_timestamp(
            col("created_date"),
            "MM/dd/yyyy hh:mm:ss a"
        )
    )
    .withColumn(
        "closed_date",
        to_timestamp(
            col("closed_date"),
            "MM/dd/yyyy hh:mm:ss a"
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Repsonse Time

# COMMAND ----------

silver_df = silver_df.withColumn(
    "response_hours",
    round(
        (
            col("closed_date").cast("long")
            - col("created_date").cast("long")
        ) / 3600,
        2
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up Boroughs

# COMMAND ----------

silver_df = silver_df.withColumn(
    "borough",
    upper(trim(col("borough")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop duplicates

# COMMAND ----------

silver_df = silver_df.dropDuplicates(
    ["unique_key"]
)

# COMMAND ----------

silver_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_311_requests")

# COMMAND ----------

display(
    spark.table("silver_311_requests")
)

# COMMAND ----------

spark.table("silver_311_requests").count()