# Databricks notebook source
assert row_count > 0

assert duplicate_count == 0

assert null_request_ids == 0

# COMMAND ----------

dq_results = {
    "row_count": row_count,
    "duplicate_count": duplicate_count,
    "null_request_ids": null_request_ids
}