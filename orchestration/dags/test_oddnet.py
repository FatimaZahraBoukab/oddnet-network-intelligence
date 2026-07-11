from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# ============================================================
# FONCTIONS PYTHON QUE LES TÂCHES VONT EXÉCUTER
# ============================================================

def dire_bonjour():
    print("Bonjour depuis Airflow ! Le pipeline ODDnet est pret.")

def verifier_config():
    print("Verification de la configuration du pipeline...")
    print("Kafka, Spark, MinIO : tout est configure.")


# ============================================================
# DÉFINITION DU DAG
# ============================================================

with DAG(
    dag_id="test_oddnet",              # nom unique du DAG (visible dans l'interface)
    start_date=datetime(2026, 7, 1),   # date à partir de laquelle le DAG peut s'exécuter
    schedule_interval="@daily",        # fréquence : une fois par jour
    catchup=False                       # ne pas rattraper les exécutions passées manquées
) as dag:

    # Première tâche
    tache_bonjour = PythonOperator(
        task_id="dire_bonjour",
        python_callable=dire_bonjour
    )

    # Deuxième tâche
    tache_verification = PythonOperator(
        task_id="verifier_config",
        python_callable=verifier_config
    )

    # ============================================================
    # DÉFINIR L'ORDRE D'EXÉCUTION
    # ============================================================
    tache_bonjour >> tache_verification   # bonjour d'abord, puis vérification