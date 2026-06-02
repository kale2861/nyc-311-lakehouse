# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Analytics
# MAGIC
# MAGIC The Gold layer contains business-ready tables created from the cleaned Silver layer.
# MAGIC
# MAGIC These tables support operational reporting around:
# MAGIC - request volume
# MAGIC - response time
# MAGIC - borough-level service patterns
# MAGIC - agency performance
# MAGIC - complaint type trends

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    avg,
    round,
    percentile_approx,
    date_trunc
)

# COMMAND ----------

silver_df = spark.table("silver_311_requests")

# COMMAND ----------

gold_borough_summary = (
    silver_df
    .groupBy("borough")
    .agg(
        count("*").alias("total_requests"),
        round(avg("response_hours"), 2).alias("avg_response_hours"),
        countDistinct("problem_formerly_complaint_type").alias("unique_complaint_types")
    )
    .orderBy(col("total_requests").desc())
)

gold_borough_summary.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_borough_summary")

display(gold_borough_summary)

# COMMAND ----------

gold_agency_performance = (
    silver_df
    .groupBy("agency", "agency_name")
    .agg(
        count("*").alias("total_requests"),
        round(avg("response_hours"), 2).alias("avg_response_hours"),
        round(percentile_approx("response_hours", 0.5), 2).alias("median_response_hours")
    )
    .orderBy(col("total_requests").desc())
)

gold_agency_performance.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_agency_performance")

display(gold_agency_performance)

# COMMAND ----------

gold_complaint_summary = (
    silver_df
    .groupBy("problem_formerly_complaint_type")
    .agg(
        count("*").alias("total_requests"),
        round(avg("response_hours"), 2).alias("avg_response_hours")
    )
    .orderBy(col("total_requests").desc())
)

gold_complaint_summary.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_complaint_summary")

display(gold_complaint_summary)

# COMMAND ----------

gold_monthly_trends = (
    silver_df
    .withColumn("request_month", date_trunc("month", col("created_date")))
    .groupBy("request_month")
    .agg(
        count("*").alias("total_requests"),
        round(avg("response_hours"), 2).alias("avg_response_hours")
    )
    .orderBy("request_month")
)

gold_monthly_trends.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_monthly_trends")

display(gold_monthly_trends)

# COMMAND ----------

gold_borough_complaint_summary = (
    silver_df
    .groupBy(
        "borough",
        "problem_formerly_complaint_type"
    )
    .agg(
        count("*").alias("total_requests"),
        round(avg("response_hours"), 2).alias("avg_response_hours")
    )
    .orderBy(
        col("borough"),
        col("total_requests").desc()
    )
)

gold_borough_complaint_summary.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_borough_complaint_summary")

display(gold_borough_complaint_summary)

# COMMAND ----------

total_records = silver_df.count()

duplicate_records = (
    silver_df
    .groupBy("unique_key")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_request_ids = (
    silver_df
    .filter(col("unique_key").isNull())
    .count()
)

gold_pipeline_metrics = spark.createDataFrame(
    [
        (
            total_records,
            duplicate_records,
            null_request_ids
        )
    ],
    [
        "total_records",
        "duplicate_records",
        "null_request_ids"
    ]
)

gold_pipeline_metrics.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_pipeline_metrics")

display(gold_pipeline_metrics)

# COMMAND ----------

spark.sql("SHOW TABLES").show()