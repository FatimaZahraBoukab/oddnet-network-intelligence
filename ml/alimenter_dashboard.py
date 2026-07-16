import pandas as pd
from sqlalchemy import create_engine

# ============================================================
# 1. CONNEXIONS
# ============================================================
STORAGE_OPTIONS = {
    "key": "minioadmin",
    "secret": "minioadmin123",
    "client_kwargs": {"endpoint_url": "http://localhost:9000"}
}

# Connexion PostgreSQL (port 5433 car mappe different, cf docker-compose)
engine = create_engine("postgresql://dashboard:dashboard@localhost:5433/oddnet_dashboard")

# ============================================================
# 2. CHARGER ET ÉCRIRE : SILVER (mesures detaillees)
# ============================================================
print("Chargement de Silver...")
df_silver = pd.read_parquet("s3://silver/network-kpis-agrege/", storage_options=STORAGE_OPTIONS)
df_silver["timestamp"] = df_silver["window"].apply(lambda w: w["start"])
df_silver["timestamp"] = pd.to_datetime(df_silver["timestamp"])
df_silver = df_silver.drop(columns=["window"])

df_silver.to_sql("silver_kpis", engine, if_exists="replace", index=False)
print(f"  {len(df_silver)} lignes ecrites dans 'silver_kpis'")

# ============================================================
# 3. CHARGER ET ÉCRIRE : GOLD (resume par client)
# ============================================================
print("Chargement de Gold...")
df_gold = pd.read_parquet("s3://gold/resume_par_client.parquet", storage_options=STORAGE_OPTIONS)

df_gold.to_sql("gold_resume_client", engine, if_exists="replace", index=False)
print(f"  {len(df_gold)} lignes ecrites dans 'gold_resume_client'")

# ============================================================
# 4. CHARGER ET ÉCRIRE : ISOLATION FOREST (anomalies)
# ============================================================
print("Chargement des resultats Isolation Forest...")
df_if = pd.read_parquet("s3://machinelearning/isolation_forest_resultats.parquet", storage_options=STORAGE_OPTIONS)

df_if.to_sql("ml_anomalies", engine, if_exists="replace", index=False)
print(f"  {len(df_if)} lignes ecrites dans 'ml_anomalies'")

# ============================================================
# 5. CHARGER ET ÉCRIRE : PROPHET (predictions)
# ============================================================
print("Chargement des predictions Prophet...")
df_prophet = pd.read_parquet("s3://machinelearning/prophet_predictions.parquet", storage_options=STORAGE_OPTIONS)

df_prophet.to_sql("ml_predictions", engine, if_exists="replace", index=False)
print(f"  {len(df_prophet)} lignes ecrites dans 'ml_predictions'")

print("\nTermine ! Toutes les donnees sont dans PostgreSQL, pretes pour Grafana.")