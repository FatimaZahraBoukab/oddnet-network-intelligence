from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "ml_scripts"))

from isolation_forest import executer_isolation_forest
from prophet_prediction import executer_prophet
from alimenter_dashboard import executer_alimentation_dashboard


with DAG(
    dag_id="lancer_ml_oddnet",
    start_date=datetime(2026, 7, 1),
    schedule_interval="0 * * * *",
    catchup=False
) as dag:

    tache_dbt = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dags/dbt_project && "
            "DBT_PROFILES_DIR=/opt/airflow/dags/dbt_profiles "
            "dbt run --project-dir /opt/airflow/dags/dbt_project"
        )
    )

    tache_isolation_forest = PythonOperator(
        task_id="isolation_forest",
        python_callable=executer_isolation_forest
    )

    tache_prophet = PythonOperator(
        task_id="prophet_prediction",
        python_callable=executer_prophet
    )

    tache_alimentation = PythonOperator(
        task_id="alimenter_dashboard",
        python_callable=executer_alimentation_dashboard
    )

    # Ordre logique : dbt cree Gold en premier,
    # puis ML tourne, puis on alimente Grafana avec tout
    tache_dbt >> [tache_isolation_forest, tache_prophet] >> tache_alimentation