# Airflow DAG dbt Generator

## Installation

Airflow with helm chart:

```bash
helm install airflow apache-airflow/airflow --namespace airflow --create-namespace -f airflow-manifest/config/values.yaml
```

## How to use it

Generate DAG:

```bash
cd dbt-manifest
python dag_generator.py
```

## DAG Visualization