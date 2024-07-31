import logging
import os
from collections import defaultdict

import yaml
from dbt.cli.main import dbtRunner
from jinja2 import Template

# Logging
logging.basicConfig(
    datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)-3s %(levelname)-8s %(message)s",
    level=logging.INFO,
)


# Paths
DBT_PROJECT_PATH = "."
DEPLOYMENT_YML_PATH = os.path.join(DBT_PROJECT_PATH, "deployment.yml")

# Template for Airflow DAG
DAG_TEMPLATE = """
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta

default_args = {
    'owner': '{{ owner }}',
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
    '{{ dag_id }}',
    default_args=default_args,
    description='{{ description }}',
    schedule_interval={%- if schedule == "None" -%}None{%- else -%}"{{ schedule }}"{%- endif -%},
    start_date=days_ago(1),
    tags={{ tags }},
) as dag:

    start = EmptyOperator(task_id='start')

    {% for model in models %}
    {{ model['task_id'] }} = PythonOperator(
        task_id='{{ model['task_id'] }}',
        python_callable=run_dbt_model,
        op_args=['{{ model['name'] }}'],
    )
    {% endfor %}

    {% for model in models %}
    {% if model['dependencies'] %}
    for dependency in {{ model['dependencies'] }}:
        ExternalTaskSensor(
            task_id='{{ model['task_id'] }}_sensor_' + dependency['task_id'],
            external_dag_id=dependency['dag_id'],
            external_task_id=dependency['task_id'],
            timeout=600,
            dag=dag
        ) >> {{ model['task_id'] }}
    {% endif %}
    {% endfor %}

    end = EmptyOperator(task_id='end')

    (end <<
    {% for model in models %}
    {{ model['task_id'] }} <<
    {% endfor %}
    start)
"""


class DBTInvoker:
    def __init__(self, model: str, target: str = "prod") -> None:
        self.dbt = dbtRunner()
        self.model = model
        self.target = target

    def compile(self):
        cli_args = [
            "--log-level=none",
            "compile",
            "--select",
            self.model,
            "-t",
            self.target,
        ]
        res = self.dbt.invoke(cli_args)
        return res

    def ls(self, depth: int = 1):
        cli_args = [
            "--log-level=none",
            "ls",
            "--select",
            self.model + f"+{depth}" if depth else "",
            "--resource-type=model",
            "-t",
            self.target,
        ]
        res = self.dbt.invoke(cli_args)
        return res


class DBTExtractor:
    def __init__(self, model: str, target: str = "prod") -> None:
        self.dbt = DBTInvoker(model=model, target=target)
        self.model = model
        self.dataset_info = defaultdict(dict)

    def get_model_info(self):
        metadata = self.dbt.compile().result.results
        for info in metadata:
            self.dataset_info[info.node.unique_id] = {
                "database": info.node.database,
                "schema": info.node.schema,
                "name": info.node.name,
                "alias": info.node.alias,
                "depends_on": info.node.depends_on.nodes,
            }
        return self.dataset_info

    def get_model_dependencies(self):
        self.get_model_info()
        dependencies = []
        for _, v in self.dataset_info.items():
            for dep in v.get("depends_on"):
                if dep.startswith("model."):
                    dependencies.append(dep)
        return [f.split(".")[-1] for f in dependencies]

    def get_downstream_model(self, depth: int = 1):
        metadata = self.dbt.ls(depth=depth).result
        return metadata


def dag_generator():
    # Read deployment.yml
    with open(DEPLOYMENT_YML_PATH, "r") as file:
        deployment_config = yaml.safe_load(file)

    # Generate DAG files
    for model_group in deployment_config["model_groups"]:
        group_name = model_group["name"]
        schedule = model_group["schedule"]
        owner = model_group["owner"]
        tags = model_group["tags"]
        description = model_group["description"]

        models = []
        group_path = os.path.join(DBT_PROJECT_PATH, "models", group_name)
        for root, _, files in os.walk(group_path):
            for file in files:
                if file.endswith(".sql"):
                    model_name = file.replace(".sql", "")
                    model_file = os.path.join(root, file)
                    dbt_extractor = DBTExtractor(model=model_file, target="prod")
                    dependencies = dbt_extractor.get_model_dependencies()
                    models.append(
                        {
                            "task_id": model_name,
                            "name": model_name,
                            "dependencies": [
                                {"dag_id": f"dag_dep_{dep}", "task_id": f"dep_{dep}"}
                                for dep in dependencies
                            ],
                        }
                    )

        dag_id = "dag_" + group_name

        template = Template(DAG_TEMPLATE)
        dag_content = template.render(
            dag_id=dag_id,
            owner=owner,
            schedule=schedule,
            description=description,
            tags=tags,
            models=models,
        )

        dag_file_path = f"../airflow-manifest/dags/{dag_id}.py"
        with open(dag_file_path, "w") as dag_file:
            dag_file.write(dag_content)

        logging.info(f"DAG file {dag_file_path} generated.")


def main():
    # dbt_extractor = DBTExtractor(model="./models/ds_feature_store/pma_final.sql")
    # data = dbt_extractor.get_model_info()
    # print(data)
    # print(dbt_extractor.get_model_dependencies())
    dag_generator()
    return


if __name__ == "__main__":
    main()
