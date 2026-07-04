# Databricks notebook source
# TASK 4 of the job: silver_transform
#
# Cleans bronze data and merges (upserts) it into silver so re-runs / late
# arriving duplicates don't create duplicate rows. Also enriches transactions
# with the dimension data (denormalized, analytics-friendly silver layer).

from pyspark.sql import functions as F
from delta.tables import DeltaTable

catalog = "bank_lakehouse"

# ---------------------------------------------------------------------------
# 1. Silver dimensions - light cleanup, simple overwrite (small tables)
# ---------------------------------------------------------------------------
silver_branches_df = spark.table(f"{catalog}.bronze.bronze_branches") \
    .withColumn("branch_name", F.trim("branch_name")) \
    .dropDuplicates(["branch_id"])
silver_branches_df.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.silver.silver_branches")

silver_accounts_df = spark.table(f"{catalog}.bronze.bronze_accounts") \
    .withColumn("customer_name", F.trim("customer_name")) \
    .withColumn("account_type", F.upper("account_type")) \
    .dropDuplicates(["account_id"])
silver_accounts_df.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.silver.silver_accounts")

# ---------------------------------------------------------------------------
# 2. Silver transactions - clean, dedup, enrich, then MERGE (upsert)
# ---------------------------------------------------------------------------
bronze_txn_df = spark.table(f"{catalog}.bronze.bronze_transactions")

cleaned_df = (bronze_txn_df
    .filter(F.col("status") == "COMPLETED")          # drop failed transactions
    .filter(F.col("amount") > 0)                       # basic sanity filter
    .withColumn("transaction_ts", F.col("transaction_ts").cast("timestamp"))
    .withColumn("channel", F.upper("channel"))
    .dropDuplicates(["transaction_id"]))                # guard against dup loads

branches_lkp = spark.table(f"{catalog}.silver.silver_branches")
accounts_lkp = spark.table(f"{catalog}.silver.silver_accounts")

enriched_df = (cleaned_df
    .join(accounts_lkp, "account_id", "left")
    .join(branches_lkp, "branch_id", "left")
    .select(
        cleaned_df.transaction_id,
        cleaned_df.account_id,
        accounts_lkp.customer_name,
        accounts_lkp.account_type,
        branches_lkp.branch_id,
        branches_lkp.branch_name,
        branches_lkp.city,
        cleaned_df.transaction_type,
        cleaned_df.amount,
        cleaned_df.channel,
        cleaned_df.transaction_ts,
    ))

target_table = f"{catalog}.silver.silver_transactions"
if spark.catalog.tableExists(target_table):
    delta_target = DeltaTable.forName(spark, target_table)
    (delta_target.alias("t")
        .merge(enriched_df.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
else:
    enriched_df.write.format("delta").saveAsTable(target_table)

print(f"silver_transactions upserted, {enriched_df.count()} rows processed this run")
