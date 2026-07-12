from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime

with DAG(
    dag_id="lancer_dbt_gold",
    start_date=datetime(2026, 7, 1),
    schedule_interval="0 * * * *",   # toutes les heures, a la minute 0
    catchup=False
) as dag:

    tache_dbt_run = DockerOperator(
        task_id="dbt_run",
        image="oddnet-dbt",
        command="run --project-dir oddnet_transformation",
        docker_url="unix://var/run/docker.sock",
        network_mode="infra_default",
        mounts=[
            {
                "source": "/home/fatim/oddnet-pfa/transformation",
                "target": "/usr/app",
                "type": "bind"
            },
            {
                "source": "/home/fatim/oddnet-pfa/transformation/dbt_profiles",
                "target": "/root/.dbt",
                "type": "bind"
            }
        ],
        auto_remove=True
    )