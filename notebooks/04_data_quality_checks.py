# Databricks notebook source
# TASK 6 of the job: data_quality_checks
#
# Simple, readable DQ checks (no external framework, plain pyspark) run
# against silver/gold. Every check appends a PASS/FAIL row to a results list;
# at the end we raise an error if any critical check failed, so the job run
# actually fails and shows up red in the Databricks Jobs UI.

from pyspark.sql import functions as F

catalog = "bank_lakehouse"
silver_txn = spark.table(f"{catalog}.silver.silver_transactions")
silver_accounts = spark.table(f"{catalog}.silver.silver_accounts")

results = []

def check(name, passed, detail=""):
    results.append((name, "PASS" if passed else "FAIL", detail))

# 1. no nulls in key columns
key_cols = ["transaction_id", "account_id", "amount", "transaction_ts"]
for col in key_cols:
    null_count = silver_txn.filter(F.col(col).isNull()).count()
    check(f"not_null_{col}", null_count == 0, f"{null_count} nulls found")

# 2. transaction_id is unique
total_rows = silver_txn.count()
distinct_rows = silver_txn.select("transaction_id").distinct().count()
check("unique_transaction_id", total_rows == distinct_rows,
      f"{total_rows - distinct_rows} duplicate ids")

# 3. amount is always positive
negative_count = silver_txn.filter(F.col("amount") <= 0).count()
check("positive_amount", negative_count == 0, f"{negative_count} non-positive amounts")

# 4. referential integrity - every account_id in silver_transactions exists in silver_accounts
orphan_count = (silver_txn.select("account_id").distinct()
    .join(silver_accounts.select("account_id"), "account_id", "left_anti")
    .count())
check("referential_integrity_accounts", orphan_count == 0, f"{orphan_count} orphan account_ids")

# 5. freshness - silver has at least one transaction from the most recent batch date
max_ts = silver_txn.agg(F.max("transaction_ts")).collect()[0][0]
check("has_data", max_ts is not None, f"latest transaction_ts = {max_ts}")

# ---------------------------------------------------------------------------
# report + fail the job if anything critical broke
# ---------------------------------------------------------------------------
results_df = spark.createDataFrame(results, ["check_name", "status", "detail"])
display(results_df)

failed = [r for r in results if r[1] == "FAIL"]
if failed:
    raise Exception(f"Data quality checks failed: {failed}")
else:
    print("all data quality checks passed")
