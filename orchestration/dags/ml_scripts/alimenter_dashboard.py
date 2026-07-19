import pandas as pd
from sqlalchemy import create_engine

STORAGE_OPTIONS = {
    "key": "minioadmin",
    "secret": "minioadmin123",
    "client_kwargs": {"endpoint_url": "http://minio:9000"}
}


def executer_alimentation_dashboard():
    """
    Copie les donnees de MinIO (Silver, Gold, ML) vers PostgreSQL,
    pour que Grafana affiche des donnees a jour.
    """
    engine = create_engine("postgresql://dashboard:dashboard@postgres-dashboard:5432/oddnet_dashboard")

    print("Chargement de Silver...")
    df_silver = pd.read_parquet("s3://silver/network-kpis-agrege/", storage_options=STORAGE_OPTIONS)
    df_silver["timestamp"] = df_silver["window"].apply(lambda w: w["start"])
    df_silver["timestamp"] = pd.to_datetime(df_silver["timestamp"])
    df_silver = df_silver.drop(columns=["window"])
    df_silver.to_sql("silver_kpis", engine, if_exists="replace", index=False)
    print(f"  {len(df_silver)} lignes ecrites dans 'silver_kpis'")

    print("Chargement de Gold...")
    df_gold = pd.read_parquet("s3://gold/resume_par_client.parquet", storage_options=STORAGE_OPTIONS)
    df_gold.to_sql("gold_resume_client", engine, if_exists="replace", index=False)
    print(f"  {len(df_gold)} lignes ecrites dans 'gold_resume_client'")

    print("Chargement des resultats Isolation Forest...")
    df_if = pd.read_parquet("s3://machinelearning/isolation_forest_resultats.parquet", storage_options=STORAGE_OPTIONS)
    df_if.to_sql("ml_anomalies", engine, if_exists="replace", index=False)
    print(f"  {len(df_if)} lignes ecrites dans 'ml_anomalies'")

    print("Chargement des predictions Prophet...")
    df_prophet = pd.read_parquet("s3://machinelearning/prophet_predictions.parquet", storage_options=STORAGE_OPTIONS)
    df_prophet.to_sql("ml_predictions", engine, if_exists="replace", index=False)
    print(f"  {len(df_prophet)} lignes ecrites dans 'ml_predictions'")

    print("Termine ! Toutes les donnees sont dans PostgreSQL.")
    return len(df_silver) + len(df_gold) + len(df_if) + len(df_prophet)


if __name__ == "__main__":
    executer_alimentation_dashboard()