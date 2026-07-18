from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "ml_scripts"))

from isolation_forest import executer_isolation_forest
from prophet_prediction import executer_prophet


with DAG(
    dag_id="lancer_ml_oddnet",
    start_date=datetime(2026, 7, 1),
    schedule_interval="0 * * * *",
    catchup=False
) as dag:

    tache_isolation_forest = PythonOperator(
        task_id="isolation_forest",
        python_callable=executer_isolation_forest
    )

    tache_prophet = PythonOperator(
        task_id="prophet_prediction",
        python_callable=executer_prophet
    )