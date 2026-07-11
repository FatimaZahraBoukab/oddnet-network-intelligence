from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
import boto3

# ============================================================
# CONFIGURATION DE CONNEXION À MINIO
# ============================================================
# Mêmes identifiants que ceux utilisés par Spark

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET_SILVER = "silver"
PREFIX_A_VERIFIER = "network-kpis-agrege/"

# Combien de temps sans nouvelle donnée avant de considérer le pipeline "en panne"
SEUIL_ALERTE_MINUTES = 5


def verifier_fraicheur_silver():
    """
    Se connecte à MinIO, regarde le fichier le plus récent dans Silver,
    et vérifie s'il a été écrit récemment.
    """
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # Liste tous les objets dans le dossier Silver concerné
    reponse = client.list_objects_v2(Bucket=BUCKET_SILVER, Prefix=PREFIX_A_VERIFIER)

    if "Contents" not in reponse:
        raise Exception("ALERTE: Aucune donnee trouvee dans Silver ! Le pipeline n'a peut-etre jamais tourne.")

    # Trouve le fichier le plus récemment modifié
    fichiers = reponse["Contents"]
    dernier_fichier = max(fichiers, key=lambda f: f["LastModified"])
    derniere_maj = dernier_fichier["LastModified"]

    maintenant = datetime.now(timezone.utc)
    ecart = maintenant - derniere_maj
    ecart_minutes = ecart.total_seconds() / 60

    print(f"Dernier fichier Silver: {dernier_fichier['Key']}")
    print(f"Derniere ecriture il y a {ecart_minutes:.1f} minutes")

    if ecart_minutes > SEUIL_ALERTE_MINUTES:
        raise Exception(
            f"ALERTE: Aucune nouvelle donnee depuis {ecart_minutes:.1f} minutes. "
            f"Le pipeline Spark est peut-etre arrete !"
        )

    print("Pipeline en bonne sante: donnees recentes detectees.")


# ============================================================
# DÉFINITION DU DAG
# ============================================================

with DAG(
    dag_id="verifier_pipeline_oddnet",
    start_date=datetime(2026, 7, 1),
    schedule_interval="*/10 * * * *",   # toutes les 10 minutes
    catchup=False
) as dag:

    tache_verification = PythonOperator(
        task_id="verifier_fraicheur_silver",
        python_callable=verifier_fraicheur_silver
    )