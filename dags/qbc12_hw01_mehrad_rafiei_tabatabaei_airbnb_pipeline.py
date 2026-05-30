
"""
DAG for refreshing Airbnb materialized view and data validation.
Credentials are read from Airflow Variables.
"""
from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging

default_args = {
    'owner': 'mehrad_rafiei_tabatabaei',
    'retries': 1,
    'start_date': datetime(2025, 1, 1),
}

DAG_ID = 'qbc12_hw01_mehrad_rafiei_tabatabaei_airbnb_pipeline'
SCHEMA_NAME = 'student_mehrad_rafiei_tabatabaei'
MV_NAME = 'mv_airbnb_summary'   
def get_db_config():
    """Fetch PostgreSQL credentials from Airflow Variables."""
    return {
        'host': Variable.get('mehrad_db_host'),
        'port': Variable.get('mehrad_db_port'),
        'database': Variable.get('mehrad_db_name'),
        'user': Variable.get('mehrad_db_user'),
        'password': Variable.get('mehrad_db_password'),
    }

def read_config(**context):
    """Read DB config and push to XCom."""
    config = get_db_config()
    config['schema'] = SCHEMA_NAME
    config['mv_name'] = MV_NAME
    context['ti'].xcom_push(key='db_config', value=config)
    logging.info("Config read from Variables")
    return config

def refresh_summary(**context):
    """Refresh materialized view directly in PostgreSQL."""
    ti = context['ti']
    config = ti.xcom_pull(task_ids='read_config', key='db_config')
    hook = PostgresHook(
        postgres_conn_id='postgres_default',
        host=config['host'],
        port=config['port'],
        dbname=config['database'],
        user=config['user'],
        password=config['password'],
        schema=config['schema'],
    )
    sql = f"REFRESH MATERIALIZED VIEW CONCURRENTLY {config['schema']}.{config['mv_name']};"
    hook.run(sql)
    logging.info(f"Refreshed {config['schema']}.{config['mv_name']}")
    return {'refreshed_at': str(datetime.utcnow()), 'mv': config['mv_name']}

def validate_summary(**context):
    """Run validation checks and return pass/fail status."""
    ti = context['ti']
    config = ti.xcom_pull(task_ids='read_config', key='db_config')
    hook = PostgresHook(
        postgres_conn_id='postgres_default',
        host=config['host'],
        port=config['port'],
        dbname=config['database'],
        user=config['user'],
        password=config['password'],
        schema=config['schema'],
    )
    checks = {}

    # row_count > 0
    sql = f"SELECT COUNT(*) FROM {config['schema']}.{config['mv_name']};"
    row_count = hook.get_first(sql)[0]
    checks['row_count_positive'] = row_count > 0

    # null_neighbourhoods == 0
    sql = f"SELECT COUNT(*) FROM {config['schema']}.{config['mv_name']} WHERE neighbourhood IS NULL;"
    null_neigh = hook.get_first(sql)[0]
    checks['null_neighbourhoods_zero'] = (null_neigh == 0)

    # bad_prices == 0 (price <= 0 or NULL)
    sql = f"SELECT COUNT(*) FROM {config['schema']}.{config['mv_name']} WHERE price <= 0 OR price IS NULL;"
    bad_prices = hook.get_first(sql)[0]
    checks['bad_prices_zero'] = (bad_prices == 0)

    # bad_availability == 0 (availability_365 not in [0,365])
    sql = f"SELECT COUNT(*) FROM {config['schema']}.{config['mv_name']} WHERE availability_365 < 0 OR availability_365 > 365 OR availability_365 IS NULL;"
    bad_avail = hook.get_first(sql)[0]
    checks['bad_availability_zero'] = (bad_avail == 0)

    all_passed = all(checks.values())
    result = {'passed': all_passed, 'details': checks, 'row_count': row_count}
    ti.xcom_push(key='validation_result', value=result)
    logging.info(f"Validation result: {result}")
    return result

def choose_report_path(**context):
    """Branch to success or failure based on validation."""
    ti = context['ti']
    val_result = ti.xcom_pull(task_ids='validate_summary', key='validation_result')
    if val_result and val_result.get('passed'):
        return 'write_success_report'
    else:
        return 'write_failure_report'

def write_success_report(**context):
    """Write success report to reports/ directory."""
    ti = context['ti']
    config = ti.xcom_pull(task_ids='read_config', key='db_config')
    val_result = ti.xcom_pull(task_ids='validate_summary', key='validation_result')
    report_path = Path('/opt/airflow/reports/hw01_c_airflow.md')
    content = f"""# Airflow Pipeline Report (Success)
- DAG ID: {DAG_ID}
- Run timestamp: {datetime.utcnow()}
- Materialized view: {config['schema']}.{config['mv_name']}
- Validation passed: True
- Row count: {val_result.get('row_count')}
- Check details: {val_result.get('details')}
"""
    report_path.parent.mkdir(exist_ok=True)
    report_path.write_text(content)
    logging.info(f"Success report written to {report_path}")

def write_failure_report(**context):
    """Write failure report then raise ValueError."""
    ti = context['ti']
    config = ti.xcom_pull(task_ids='read_config', key='db_config')
    val_result = ti.xcom_pull(task_ids='validate_summary', key='validation_result')
    report_path = Path('/opt/airflow/reports/hw01_c_airflow.md')
    failed = {k:v for k,v in val_result.get('details',{}).items() if not v}
    content = f"""# Airflow Pipeline Report (FAILURE)
- DAG ID: {DAG_ID}
- Run timestamp: {datetime.utcnow()}
- Materialized view: {config['schema']}.{config['mv_name']}
- Validation passed: False
- Failed checks: {failed}
"""
    report_path.parent.mkdir(exist_ok=True)
    report_path.write_text(content)
    logging.error("Validation failed, raising exception")
    raise ValueError("Data validation failed. See report.")

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['hw01', 'airbnb'],
) as dag:
    task_read_config = PythonOperator(task_id='read_config', python_callable=read_config)
    task_refresh = PythonOperator(task_id='refresh_summary', python_callable=refresh_summary)
    task_validate = PythonOperator(task_id='validate_summary', python_callable=validate_summary)
    task_branch = BranchPythonOperator(task_id='choose_report_path', python_callable=choose_report_path)
    task_success = PythonOperator(task_id='write_success_report', python_callable=write_success_report)
    task_failure = PythonOperator(task_id='write_failure_report', python_callable=write_failure_report)

    task_read_config >> task_refresh >> task_validate >> task_branch
    task_branch >> task_success
    task_branch >> task_failure
