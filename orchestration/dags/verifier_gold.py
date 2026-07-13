from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import boto3

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET_GOLD = "gold"

# Gold est mis a jour manuellement (dbt run), donc seuil plus large
SEUIL_ALERTE_HEURES = 24


def verifier_fraicheur_gold():
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    reponse = client.list_objects_v2(Bucket=BUCKET_GOLD)

    if "Contents" not in reponse:
        raise Exception("ALERTE: Aucune donnee trouvee dans Gold ! dbt n'a peut-etre jamais tourne.")

    fichiers = reponse["Contents"]
    dernier_fichier = max(fichiers, key=lambda f: f["LastModified"])
    derniere_maj = dernier_fichier["LastModified"]

    maintenant = datetime.now(timezone.utc)
    ecart = maintenant - derniere_maj
    ecart_heures = ecart.total_seconds() / 3600

    print(f"Dernier fichier Gold: {dernier_fichier['Key']}")
    print(f"Derniere ecriture il y a {ecart_heures:.1f} heures")

    if ecart_heures > SEUIL_ALERTE_HEURES:
        raise Exception(
            f"ALERTE: Gold n'a pas ete mis a jour depuis {ecart_heures:.1f} heures. "
            f"Pensez a relancer dbt run !"
        )

    print("Couche Gold en bonne sante: donnees a jour.")


with DAG(
    dag_id="verifier_gold_oddnet",
    start_date=datetime(2026, 7, 1),
    schedule_interval="0 */6 * * *",   # toutes les 6 heures
    catchup=False
) as dag:

    tache_verification = PythonOperator(
        task_id="verifier_fraicheur_gold",
        python_callable=verifier_fraicheur_gold
    )