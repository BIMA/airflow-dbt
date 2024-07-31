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

![image](https://github.com/user-attachments/assets/e3b90bb4-680b-45dc-bf2d-e82365fe7452)
