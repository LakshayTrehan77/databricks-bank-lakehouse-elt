# Databricks notebook source
# TASK 3 of the job: bronze_ingest
#
# Reads the two static dimension files as a full overwrite (they're tiny),
# and reads the transactions landing folder INCREMENTALLY using a watermark
# stored in a yml file that lives inside the volume. Only rows newer than the
# stored watermark get appended to the bronze table -> this is what makes a
# batch load behave like an incremental load.

# %pip install pyyaml -q   # uncomment if pyyaml isn't already on the cluster
import yaml
from pyspark.sql import functions as F

catalog = "bank_lakehouse"
base_path = f"/Volumes/{catalog}/raw/landing_zone"
config_path = f"{base_path}/_checkpoints/watermark_config.yml"

# ---------------------------------------------------------------------------
# 1. Dimensions - small reference data, simple overwrite every run
# ---------------------------------------------------------------------------
branches_df = spark.read.option("header", True).option("inferSchema", True) \
    .csv(f"{base_path}/dimensions/branches.csv")
branches_df.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.bronze.bronze_branches")

accounts_df = spark.read.option("header", True).option("inferSchema", True) \
    .csv(f"{base_path}/dimensions/accounts.csv")
accounts_df.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.bronze.bronze_accounts")

print("bronze_branches and bronze_accounts refreshed")

# ---------------------------------------------------------------------------
# 2. Transactions - incremental, watermark-based
# ---------------------------------------------------------------------------
try:
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    config = {"tables": {"transactions": {"last_watermark": "1900-01-01T00:00:00"}}}

last_watermark = config["tables"]["transactions"]["last_watermark"]
print(f"watermark going into this run: {last_watermark}")

raw_txn_df = spark.read.option("header", True).option("inferSchema", True) \
    .csv(f"{base_path}/transactions")

# only pick up rows we haven't loaded yet
new_rows_df = raw_txn_df.filter(F.col("transaction_ts") > F.lit(last_watermark))
new_count = new_rows_df.count()
print(f"new rows discovered this run: {new_count}")

if new_count > 0:
    enriched_df = (new_rows_df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name()))

    enriched_df.write.format("delta").mode("append") \
        .saveAsTable(f"{catalog}.bronze.bronze_transactions")

    new_watermark = new_rows_df.agg(F.max("transaction_ts")).collect()[0][0]
    config["tables"]["transactions"]["last_watermark"] = str(new_watermark)

    with open(config_path, "w") as f:
        yaml.dump(config, f)

    print(f"bronze_transactions appended, watermark moved to {new_watermark}")
else:
    print("nothing new to load, watermark stays the same")
