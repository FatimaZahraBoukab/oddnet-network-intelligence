import pandas as pd
from prophet import Prophet
import logging

# Reduire les logs verbeux de Prophet/cmdstanpy
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

# ============================================================
# 1. CHARGER LES DONNÉES SILVER
# ============================================================
STORAGE_OPTIONS = {
    "key": "minioadmin",
    "secret": "minioadmin123",
    "client_kwargs": {"endpoint_url": "http://localhost:9000"}
}

df = pd.read_parquet(
    "s3://silver/network-kpis-agrege/",
    storage_options=STORAGE_OPTIONS
)

df["timestamp"] = df["window"].apply(lambda w: w["start"])
df["timestamp"] = pd.to_datetime(df["timestamp"])

print(f"Donnees chargees: {len(df)} lignes")

# ============================================================
# 2. SEUIL D'ALERTE
# ============================================================
SEUIL_ALERTE_LATENCE = 100  # ms

# ============================================================
# 3. BOUCLE SUR CHAQUE ÉQUIPEMENT
# ============================================================
equipements = df["equipement_id"].unique()
print(f"\nEquipements a traiter: {list(equipements)}\n")

toutes_les_predictions = []

for equipement in equipements:
    print(f"--- Traitement de {equipement} ---")

    df_eq = df[df["equipement_id"] == equipement].copy()
    df_eq = df_eq.sort_values("timestamp")

    # On a besoin d'un minimum de points pour que Prophet fonctionne correctement
    if len(df_eq) < 10:
        print(f"  Ignore: pas assez de donnees ({len(df_eq)} points)")
        continue

    client_id = df_eq["client_id"].iloc[0]

    df_prophet = df_eq[["timestamp", "latence_moyenne"]].rename(
        columns={"timestamp": "ds", "latence_moyenne": "y"}
    )

    modele = Prophet()
    modele.fit(df_prophet)

    futur = modele.make_future_dataframe(periods=30, freq="min")
    prediction = modele.predict(futur)

    # On ne garde que les 30 nouvelles minutes predites (pas l'historique)
    predictions_futures = prediction.tail(30).copy()
    predictions_futures["equipement_id"] = equipement
    predictions_futures["client_id"] = client_id
    predictions_futures["alerte"] = predictions_futures["yhat"] > SEUIL_ALERTE_LATENCE

    nb_alertes = predictions_futures["alerte"].sum()
    print(f"  {len(predictions_futures)} predictions generees, {nb_alertes} alertes")

    toutes_les_predictions.append(predictions_futures)

# ============================================================
# 4. RASSEMBLER TOUS LES RÉSULTATS
# ============================================================
resultat_final = pd.concat(toutes_les_predictions, ignore_index=True)

resultat_final = resultat_final[[
    "client_id", "equipement_id", "ds", "yhat", "yhat_lower", "yhat_upper", "alerte"
]].rename(columns={"ds": "timestamp_predit", "yhat": "latence_predite"})

print(f"\n=== RESUME GLOBAL ===")
print(f"Total predictions: {len(resultat_final)}")
print(f"Total alertes: {resultat_final['alerte'].sum()}")

if resultat_final["alerte"].sum() > 0:
    print("\n--- Equipements avec alertes ---")
    print(resultat_final[resultat_final["alerte"]][
        ["client_id", "equipement_id", "timestamp_predit", "latence_predite"]
    ])

# ============================================================
# 5. SAUVEGARDER DANS MINIO
# ============================================================
resultat_final.to_parquet(
    "s3://machinelearning/prophet_predictions.parquet",
    storage_options=STORAGE_OPTIONS
)

print(f"\nResultats sauvegardes dans s3://machinelearning/prophet_predictions.parquet")