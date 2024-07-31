
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta

default_args = {
    'owner': 'DS_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_dbt_model(model_name):
    # Add your logic to run dbt model
    print(f"Running dbt model: {model_name}")

with DAG(
    'dag_ds_feature_store',
    default_args=default_args,
    description='This is a DS pipeline',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['DS'],
) as dag:

    start = EmptyOperator(task_id='start')

    
    pma_final = PythonOperator(
        task_id='pma_final',
        python_callable=run_dbt_model,
        op_args=['pma_final'],
    )
    
    pma_features = PythonOperator(
        task_id='pma_features',
        python_callable=run_dbt_model,
        op_args=['pma_features'],
    )
    
    pre_pma_features = PythonOperator(
        task_id='pre_pma_features',
        python_callable=run_dbt_model,
        op_args=['pre_pma_features'],
    )
    

    
    
    for dependency in [{'dag_id': 'dag__internal_pma_features', 'task_id': 'dep__internal_pma_features'}]:
        if 'internal' in dependency['task_id']:
            continue
        ExternalTaskSensor(
            task_id='pma_final_sensor_' + dependency['task_id'],
            external_dag_id=dependency['dag_id'],
            external_task_id=dependency['task_id'],
            timeout=600,
            dag=dag
        ) >> pma_final
    
    
    
    for dependency in [{'dag_id': 'dag__internal_pre_pma_features', 'task_id': 'dep__internal_pre_pma_features'}]:
        if 'internal' in dependency['task_id']:
            continue
        ExternalTaskSensor(
            task_id='pma_features_sensor_' + dependency['task_id'],
            external_dag_id=dependency['dag_id'],
            external_task_id=dependency['task_id'],
            timeout=600,
            dag=dag
        ) >> pma_features
    
    
    
    for dependency in [{'dag_id': 'dag__external_fact_sta_withdrawals', 'task_id': 'dep__external_fact_sta_withdrawals'}, {'dag_id': 'dag__external_fact_sta_topup', 'task_id': 'dep__external_fact_sta_topup'}]:
        if 'internal' in dependency['task_id']:
            continue
        ExternalTaskSensor(
            task_id='pre_pma_features_sensor_' + dependency['task_id'],
            external_dag_id=dependency['dag_id'],
            external_task_id=dependency['task_id'],
            timeout=600,
            dag=dag
        ) >> pre_pma_features
    
    

    end = EmptyOperator(task_id='end')

    (end <<
    
    pma_final <<
    
    pma_features <<
    
    pre_pma_features <<
    
    start)