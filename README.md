
# HW01-C — Airflow Scheduled Pipeline

An automated data pipeline that refreshes a materialized view, validates data quality, and writes a run report — all managed by an Airflow DAG.

## What it does

- Reads database credentials from Airflow Variables (no hardcoded secrets).
- Refreshes a materialized view (`mv_airbnb_summary`) inside a shared PostgreSQL server.
- Runs four validation checks:
  - `row_count > 0`
  - `null_neighbourhoods == 0`
  - `bad_prices == 0`
  - `bad_availability == 0`
- Branches to a success or failure report based on validation results.
- Writes a Markdown report (`hw01_c_airflow.md`) into the `reports/` folder.

> **Note:** According to the instructor’s announcement, only the DAG file needs to be uploaded to GitHub. No actual Airflow execution or screenshots are required.

## Deliverables

- `dags/qbc12_hw01_mehrad_rafiei_tabatabaei_airbnb_pipeline.py`
- `reports/hw01_c_airflow.md`

## How to run (local verification only)

1. Install Airflow (optional — not needed for submission):
   ```bash
   pip install apache-airflow apache-airflow-providers-postgres
   ```
2. Open the notebook:
   ```bash
   jupyter notebook 03_airflow_pipeline_student.ipynb
   ```
3. Run all cells — they will generate the DAG file and the report automatically.
4. Push the generated files to GitHub.

The DAG uses the following Airflow Variables (must be set on the server if you ever run it):
- `mehrad_db_host`, `mehrad_db_port`, `mehrad_db_name`, `mehrad_db_user`, `mehrad_db_password`

And a Postgres connection with ID `postgres_default`.
