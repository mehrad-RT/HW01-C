# HW01-C Airflow Pipeline Report

- **DAG ID:** qbc12_hw01_mehrad_rafiei_tabatabaei_airbnb_pipeline
- **Airflow URL:** http://185.50.38.163:33013
- **Successful run timestamp:** 2026-05-31 00:19:54
- **Refreshed object:** student_mehrad_rafiei_tabatabaei.mv_airbnb_summary
- **Validation result:** PASSED (all checks true)
- **Screenshot paths:**
  - `screenshots/airflow_dag_graph.png`
  - `screenshots/airflow_success_run.png`

## Validation details
- row_count > 0 : True
- null_neighbourhoods == 0 : True
- bad_prices == 0 : True
- bad_availability == 0 : True

## How to reproduce
1. Unpause DAG in Airflow UI
2. Trigger DAG manually
3. Check Graph View and logs
