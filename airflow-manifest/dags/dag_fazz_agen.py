
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta

default_args = {
    'owner': 'FazzAgen_team',
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
    'dag_fazz_agen',
    default_args=default_args,
    description='This is a FA pipeline',
    schedule_interval='None',
    start_date=days_ago(1),
    tags=['fazz-agen'],
) as dag:

    start = EmptyOperator(task_id='start')

    
    fact_sta_topup = PythonOperator(
        task_id='fact_sta_topup',
        python_callable=run_dbt_model,
        op_args=['fact_sta_topup'],
    )
    
    fact_transactions = PythonOperator(
        task_id='fact_transactions',
        python_callable=run_dbt_model,
        op_args=['fact_transactions'],
    )
    
    fact_sta_withdrawals = PythonOperator(
        task_id='fact_sta_withdrawals',
        python_callable=run_dbt_model,
        op_args=['fact_sta_withdrawals'],
    )
    

    
    
    
    
    
    
    

    end = EmptyOperator(task_id='end')

    start >> fact_sta_topup >> end