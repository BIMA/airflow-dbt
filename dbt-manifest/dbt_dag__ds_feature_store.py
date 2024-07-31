from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


default_args = {
    "owner": "DS_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt__ds_feature_store_pipeline",
    default_args=default_args,
    description="This is a DS pipeline",
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False,
    tags=['DS']
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    
    with TaskGroup(group_id="ds_feature_store_pma_features") as group_ds_feature_store_pma_features:
        task_run = EmptyOperator(task_id="dbt_run_pma_features")
        task_test = EmptyOperator(task_id="dbt_test_pma_features")
        task_run >> task_test
    
    with TaskGroup(group_id="ds_feature_store_pre_pma_features") as group_ds_feature_store_pre_pma_features:
        task_run = EmptyOperator(task_id="dbt_run_pre_pma_features")
        task_test = EmptyOperator(task_id="dbt_test_pre_pma_features")
        task_run >> task_test
    
    with TaskGroup(group_id="ds_feature_store_pma_final") as group_ds_feature_store_pma_final:
        task_run = EmptyOperator(task_id="dbt_run_pma_final")
        task_test = EmptyOperator(task_id="dbt_test_pma_final")
        task_run >> task_test
    

    
    group_ds_feature_store_pma_features << group_ds_feature_store_pre_pma_features
    group_ds_feature_store_pma_final << group_ds_feature_store_pma_features