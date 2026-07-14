import pandas as pd

# ============================================================
# CONFIGURATION DE CONNEXION À MINIO
# ============================================================
STORAGE_OPTIONS = {
    "key": "minioadmin",
    "secret": "minioadmin123",
    "client_kwargs": {"endpoint_url": "http://localhost:9000"}
}

# ============================================================
# CHARGER LES DONNÉES SILVER
# ============================================================
df = pd.read_parquet(
    "s3://silver/network-kpis-agrege/",
    storage_options=STORAGE_OPTIONS
)

print("Nombre de lignes chargees:", len(df))
print("\nColonnes disponibles:")
print(df.columns.tolist())
print("\nApercu des donnees:")
print(df.head(10))
print("\nStatistiques generales:")
print(df.describe())