# Databricks notebook source
# TASK 7 of the job: analytics_queries
#
# A handful of business questions answered directly off the gold tables,
# to prove the pipeline actually produces something useful at the end.

catalog = "bank_lakehouse"
spark.sql(f"USE CATALOG {catalog}")

# 1. Top 5 branches by total transaction value
display(spark.sql("""
    SELECT branch_name, SUM(total_amount) AS total_value, SUM(txn_count) AS total_txns
    FROM gold.gold_daily_branch_summary
    GROUP BY branch_name
    ORDER BY total_value DESC
    LIMIT 5
"""))

# 2. Daily transaction trend (volume + value) across all branches
display(spark.sql("""
    SELECT txn_date, SUM(txn_count) AS daily_txns, SUM(total_amount) AS daily_value
    FROM gold.gold_daily_branch_summary
    GROUP BY txn_date
    ORDER BY txn_date
"""))

# 3. Channel preference share (%)
display(spark.sql("""
    SELECT channel,
           SUM(txn_count) AS txn_count,
           ROUND(100.0 * SUM(txn_count) / SUM(SUM(txn_count)) OVER (), 2) AS pct_share
    FROM gold.gold_channel_summary
    GROUP BY channel
    ORDER BY txn_count DESC
"""))

# 4. Accounts with negative net movement (spending more than depositing) - simple risk flag
display(spark.sql("""
    SELECT account_id, customer_name, account_type, net_movement, total_txns
    FROM gold.gold_account_summary
    WHERE net_movement < 0
    ORDER BY net_movement ASC
    LIMIT 10
"""))

# 5. Most active accounts by transaction count
display(spark.sql("""
    SELECT account_id, customer_name, total_txns, last_txn_ts
    FROM gold.gold_account_summary
    ORDER BY total_txns DESC
    LIMIT 10
"""))
