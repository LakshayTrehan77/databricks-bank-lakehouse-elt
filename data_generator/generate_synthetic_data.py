# Databricks notebook source
# TASK 2 of the job: generate_synthetic_data
#
# Generates a small, fake BANK dataset:
#   - branches.csv     -> static dimension, written once
#   - accounts.csv      -> static dimension, written once
#   - batch_00N.csv     -> a NEW file every time this notebook runs, this is
#                          what simulates a "new incremental batch landing in
#                          the lake", e.g. yesterday's transactions dropped by
#                          the core banking system.
#
# Run this notebook multiple times (or trigger the job multiple times) with a
# different `batch_id` widget value to simulate day 1, day 2, day 3 ... loads.

import random
import csv
from datetime import datetime, timedelta

dbutils.widgets.text("batch_id", "1")
batch_id = int(dbutils.widgets.get("batch_id"))

catalog = "bank_lakehouse"
base_path = f"/Volumes/{catalog}/raw/landing_zone"
random.seed(batch_id)  # keeps each batch reproducible but different from others

# ---------------------------------------------------------------------------
# 1. Static dimensions - only generated the first time (batch_id == 1)
# ---------------------------------------------------------------------------
branch_names = ["Connaught Place", "Andheri West", "Whitefield", "Salt Lake",
                 "Banjara Hills", "Koregaon Park", "Anna Nagar"]
cities = ["Delhi", "Mumbai", "Bengaluru", "Kolkata", "Hyderabad", "Pune", "Chennai"]

branches_path = f"{base_path}/dimensions/branches.csv"
if batch_id == 1:
    with open(branches_path.replace("/Volumes", "/Volumes"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["branch_id", "branch_name", "city", "region"])
        for i, name in enumerate(branch_names, start=1):
            writer.writerow([i, name, cities[i - 1], "North" if i % 2 == 0 else "South"])
    print(f"branches.csv written with {len(branch_names)} rows")

first_names = ["Aarav", "Vivaan", "Ishaan", "Ananya", "Diya", "Kabir", "Sara",
               "Rohan", "Meera", "Aditi", "Karan", "Priya", "Nikhil", "Riya"]
last_names = ["Sharma", "Verma", "Iyer", "Reddy", "Gupta", "Nair", "Khan", "Das"]
account_types = ["SAVINGS", "CURRENT", "SALARY"]

accounts_path = f"{base_path}/dimensions/accounts.csv"
num_accounts = 200
if batch_id == 1:
    with open(accounts_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["account_id", "customer_name", "account_type", "branch_id", "open_date"])
        for acc_id in range(1, num_accounts + 1):
            name = f"{random.choice(first_names)} {random.choice(last_names)}"
            acc_type = random.choice(account_types)
            branch_id = random.randint(1, len(branch_names))
            open_date = (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 700))).date()
            writer.writerow([acc_id, name, acc_type, branch_id, open_date])
    print(f"accounts.csv written with {num_accounts} rows")

# ---------------------------------------------------------------------------
# 2. Incremental transaction batch - a brand new file every run
# ---------------------------------------------------------------------------
txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "BILL_PAYMENT"]
channels = ["ATM", "ONLINE", "POS", "BRANCH", "UPI"]
statuses = ["COMPLETED", "COMPLETED", "COMPLETED", "FAILED"]  # ~75% completed

rows_per_batch = 500
# each batch represents "one more day" of transactions
batch_day = datetime(2024, 1, 1) + timedelta(days=batch_id - 1)

batch_file = f"{base_path}/transactions/batch_{batch_id:03d}.csv"
with open(batch_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["transaction_id", "account_id", "transaction_type", "amount",
                      "channel", "status", "transaction_ts"])
    for i in range(1, rows_per_batch + 1):
        transaction_id = f"TXN-{batch_id:03d}-{i:05d}"
        account_id = random.randint(1, num_accounts)
        txn_type = random.choice(txn_types)
        amount = round(random.uniform(50, 50000), 2)
        channel = random.choice(channels)
        status = random.choice(statuses)
        # spread transactions across the simulated day, seconds apart
        ts = batch_day + timedelta(seconds=random.randint(0, 86399))
        writer.writerow([transaction_id, account_id, txn_type, amount, channel,
                          status, ts.strftime("%Y-%m-%dT%H:%M:%S")])

print(f"batch_{batch_id:03d}.csv written with {rows_per_batch} new transactions "
      f"(simulated day: {batch_day.date()})")
