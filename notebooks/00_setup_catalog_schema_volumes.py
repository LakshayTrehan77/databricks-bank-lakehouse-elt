# Databricks notebook source
# TASK 1 of the job: setup_infra
# Run once (or run every time, it is idempotent) before the pipeline starts.
# Creates the catalog, the 4 schemas (raw, bronze, silver, gold) and the
# volume we use as the "landing zone" where synthetic batch files will land.

catalog = "bank_lakehouse"
schemas = ["raw", "bronze", "silver", "gold"]

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# volume that stores the raw incoming files (this is our simulated "data lake landing zone")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.raw.landing_zone")

# pre-create the folders we will write to, so later notebooks don't need to worry about it
dbutils.fs.mkdirs(f"/Volumes/{catalog}/raw/landing_zone/dimensions")
dbutils.fs.mkdirs(f"/Volumes/{catalog}/raw/landing_zone/transactions")
dbutils.fs.mkdirs(f"/Volumes/{catalog}/raw/landing_zone/_checkpoints")

print(f"catalog '{catalog}', schemas {schemas} and the landing_zone volume are ready")
