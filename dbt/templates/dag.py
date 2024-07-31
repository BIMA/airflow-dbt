from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


default_args = {
    "owner": "{{ owner }}",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt__{{ name }}_pipeline",
    default_args=default_args,
    description="{{ description }}",
    schedule_interval={%- if schedule_interval == "None" -%}None{%- else -%}"{{ schedule_interval }}"{%- endif -%},
    start_date=days_ago(2),
    catchup=False,
    tags={{ tags }}
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    {% for model, node in nodes|items %}
    with TaskGroup(group_id="{{ name }}_{{ model }}") as group_{{ name }}_{{ model }}:
        task_run = EmptyOperator(task_id="dbt_run_{{ model }}")
        task_test = EmptyOperator(task_id="dbt_test_{{ model }}")
        task_run >> task_test
    {% endfor %}

    {% for model, node in nodes|items %}
    {%- for upstream in node.inner_upstream %}
    group_{{ name }}_{{ model }} << group_{{ name }}_{{ upstream }}
    {%- endfor -%}
    {% endfor %}
