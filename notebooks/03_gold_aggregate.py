# Databricks notebook source
# TASK 5 of the job: gold_aggregate
#
# Builds the business-facing aggregate tables on top of silver. These are
# small, fully recomputed each run (simplest correct approach for aggregates
# over a modest silver table); for very large fact tables you'd switch this
# to an incremental merge too, same pattern as the silver notebook.

from pyspark.sql import functions as F

catalog = "bank_lakehouse"
silver_txn = spark.table(f"{catalog}.silver.silver_transactions")

# 1. Daily volume & value per branch
daily_branch_summary_df = (silver_txn
    .withColumn("txn_date", F.to_date("transaction_ts"))
    .groupBy("txn_date", "branch_id", "branch_name")
    .agg(
        F.count("transaction_id").alias("txn_count"),
        F.sum("amount").alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    ))
daily_branch_summary_df.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.gold_daily_branch_summary")

# 2. Per-account net movement (deposits/transfers-in count as +, rest as -)
account_summary_df = (silver_txn
    .withColumn("signed_amount",
        F.when(F.col("transaction_type").isin("DEPOSIT"), F.col("amount"))
         .otherwise(-F.col("amount")))
    .groupBy("account_id", "customer_name", "account_type")
    .agg(
        F.count("transaction_id").alias("total_txns"),
        F.round(F.sum("signed_amount"), 2).alias("net_movement"),
        F.max("transaction_ts").alias("last_txn_ts"),
    ))
account_summary_df.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.gold_account_summary")

# 3. Channel usage split, daily
channel_summary_df = (silver_txn
    .withColumn("txn_date", F.to_date("transaction_ts"))
    .groupBy("txn_date", "channel")
    .agg(
        F.count("transaction_id").alias("txn_count"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
    ))
channel_summary_df.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.gold_channel_summary")

print("gold_daily_branch_summary, gold_account_summary, gold_channel_summary refreshed")
