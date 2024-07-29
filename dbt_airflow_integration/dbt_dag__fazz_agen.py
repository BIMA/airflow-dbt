from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


default_args = {
    "owner": "FazzAgen_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt__fazz_agen_pipeline",
    default_args=default_args,
    description="This is a FA pipeline",
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False,
    tags=['fazz-agen']
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    
    with TaskGroup(group_id="fazz_agen_fact_sta_withdrawals") as group_fazz_agen_fact_sta_withdrawals:
        task_run = EmptyOperator(task_id="dbt_run_fact_sta_withdrawals")
        task_test = EmptyOperator(task_id="dbt_test_fact_sta_withdrawals")
        task_run >> task_test
    
    with TaskGroup(group_id="fazz_agen_fact_sta_topup") as group_fazz_agen_fact_sta_topup:
        task_run = EmptyOperator(task_id="dbt_run_fact_sta_topup")
        task_test = EmptyOperator(task_id="dbt_test_fact_sta_topup")
        task_run >> task_test
    
    with TaskGroup(group_id="fazz_agen_fact_transactions") as group_fazz_agen_fact_transactions:
        task_run = EmptyOperator(task_id="dbt_run_fact_transactions")
        task_test = EmptyOperator(task_id="dbt_test_fact_transactions")
        task_run >> task_test
    

    