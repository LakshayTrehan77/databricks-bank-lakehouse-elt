# Bank Lakehouse ELT (Databricks, PySpark)

A Databricks Lakehouse project. It simulates a bank's
transaction feed landing in batches, and moves it through a **Bronze -> Silver
-> Gold** medallion pipeline using **watermark-based incremental loading**,
Unity Catalog **volumes/schemas**, a few **data quality checks**, and some
**analytics queries** at the end.

No real data, no e-commerce — synthetic bank accounts, branches and
transactions, generated inside the project itself.

## Folder structure

```
databricks-bank-lakehouse-elt/
├── README.md
├── config/
│   └── watermark_config.yml        # template of the watermark contract (live copy lives in the Volume)
├── data_generator/
│   └── generate_synthetic_data.py  # writes fake dimension + a new incremental batch file each run
├── notebooks/
│   ├── 00_setup_catalog_schema_volumes.py
│   ├── 01_bronze_ingest_incremental.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_aggregate.py
│   ├── 04_data_quality_checks.py
│   └── 05_analytics_queries.py
├── jobs/
│   └── databricks_job.yml          # Databricks Job / Asset Bundle task graph
└── docs/
    └── data_lineage.md             # lineage table + mermaid diagram
```

## Which file attaches to which Databricks job task

| # | Task key                | Notebook / file to attach                        |
|---|--------------------------|----------------------------------------------------|
| 1 | `setup_infra`            | `notebooks/00_setup_catalog_schema_volumes.py`      |
| 2 | `generate_synthetic_data`| `data_generator/generate_synthetic_data.py`         |
| 3 | `bronze_ingest`           | `notebooks/01_bronze_ingest_incremental.py`         |
| 4 | `silver_transform`        | `notebooks/02_silver_transform.py`                  |
| 5 | `gold_aggregate`          | `notebooks/03_gold_aggregate.py`                    |
| 6 | `data_quality_checks`     | `notebooks/04_data_quality_checks.py`               |
| 7 | `analytics_queries`       | `notebooks/05_analytics_queries.py`                 |

Each task depends on the one before it — see `jobs/databricks_job.yml` for the
exact dependency graph, or recreate it by hand in Workflows > Jobs using the
table above as the order.

## Data model

**Dimensions** (static, full-refresh each run): `branches`, `accounts`
**Fact** (incremental, append + watermark): `transactions`

```
branches (branch_id, branch_name, city, region)
accounts (account_id, customer_name, account_type, branch_id, open_date)
transactions (transaction_id, account_id, transaction_type, amount, channel, status, transaction_ts)
```

## How the incremental / watermark loading works

1. `generate_synthetic_data.py` is run with a `batch_id` widget (1, 2, 3, ...).
   Each run writes **one new file**, `batch_00N.csv`, into the volume — this
   is the stand-in for a new day's file landing in a real data lake.
2. `01_bronze_ingest_incremental.py` reads a small YAML checkpoint
   (`_checkpoints/watermark_config.yml`, stored inside the Volume so it
   survives across job runs), keeps only rows where
   `transaction_ts > last_watermark`, appends those to
   `bronze.bronze_transactions`, then writes the new max watermark back to
   the YAML.
3. `02_silver_transform.py` cleans the bronze data and **merges (upserts)**
   it into silver on `transaction_id`, so reprocessing a file never creates
   duplicates.

This means you can run the whole job multiple times with an increasing
`batch_id` and watch bronze/silver/gold grow incrementally, exactly like a
real daily batch pipeline.

## Setup / how to run

1. Create a Databricks workspace with **Unity Catalog** enabled (any cloud).
2. Upload this folder as a Databricks Repo (Repos > Add Repo > upload/git),
   or import each notebook individually.
3. Make sure `pyyaml` is available on the cluster (it usually already is on
   the standard Databricks Runtime; if not, uncomment the `%pip install
   pyyaml` line at the top of `01_bronze_ingest_incremental.py`).
4. Create a Job from `jobs/databricks_job.yml` (via `databricks bundle
   deploy`) or recreate the 7 tasks manually in the Jobs UI using the table
   above.
5. Run the job with `batch_id = 1` the first time (creates dimensions +
   first batch). Run it again with `batch_id = 2`, then `3`, etc., to
   simulate more incoming batches — bronze/silver/gold will grow each time.

## Data quality checks (Task 6)

Plain PySpark, no external framework:
- not-null checks on key columns
- `transaction_id` uniqueness
- `amount > 0` sanity check
- referential integrity: every `account_id` in silver transactions exists in
  silver accounts
- freshness: silver has a valid max `transaction_ts`

The job task fails loudly (raises an exception) if any critical check fails,
so a broken run shows up red in the Databricks Jobs UI instead of silently
producing bad gold tables.

## Analytics queries (Task 7)

Five sample business questions run straight off the gold tables: top
branches by value, daily transaction trend, channel share, accounts with
negative net movement, and most active accounts.

## Lineage

See `docs/data_lineage.md` for the full task-to-table lineage map and a
mermaid diagram of the whole pipeline.
