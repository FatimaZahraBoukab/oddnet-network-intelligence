import pandas as pd
from prophet import Prophet
import logging

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
# 2. MÉTRIQUES À PRÉDIRE + LEURS SEUILS D'ALERTE
# ============================================================
# Chaque metrique a sa propre colonne source et son propre seuil.
# "sens" indique si l'alerte se declenche AU-DESSUS ou EN-DESSOUS du seuil.

METRIQUES = {
    "latence_moyenne": {
        "seuil": 100,
        "sens": "au_dessus",   # alerte si latence PREVUE > 100ms
        "nom_colonne_predite": "latence_predite"
    },
    "debit_moyen": {
        "seuil": 50,
        "sens": "en_dessous",  # alerte si debit PREVU < 50 Mbps
        "nom_colonne_predite": "debit_predit"
    },
    "disponibilite_moyenne": {
        "seuil": 95,
        "sens": "en_dessous",  # alerte si disponibilite PREVUE < 95%
        "nom_colonne_predite": "disponibilite_predite"
    }
}

# ============================================================
# 3. FONCTION QUI ENTRAINE PROPHET SUR UNE METRIQUE DONNEE
# ============================================================
def predire_une_metrique(df_equipement, colonne_source, config):
    df_prophet = df_equipement[["timestamp", colonne_source]].rename(
        columns={"timestamp": "ds", colonne_source: "y"}
    )

    modele = Prophet()
    modele.fit(df_prophet)

    futur = modele.make_future_dataframe(periods=30, freq="min")
    prediction = modele.predict(futur)

    predictions_futures = prediction.tail(30)[["ds", "yhat"]].copy()
    predictions_futures = predictions_futures.rename(
        columns={"ds": "timestamp_predit", "yhat": config["nom_colonne_predite"]}
    )

    # Calcul de l'alerte selon le sens defini pour cette metrique
    if config["sens"] == "au_dessus":
        predictions_futures["alerte_" + colonne_source] = (
            predictions_futures[config["nom_colonne_predite"]] > config["seuil"]
        )
    else:
        predictions_futures["alerte_" + colonne_source] = (
            predictions_futures[config["nom_colonne_predite"]] < config["seuil"]
        )

    return predictions_futures


# ============================================================
# 4. BOUCLE PRINCIPALE : POUR CHAQUE EQUIPEMENT, POUR CHAQUE METRIQUE
# ============================================================
equipements = df["equipement_id"].unique()
print(f"\nEquipements a traiter: {list(equipements)}\n")

toutes_les_predictions = []

for equipement in equipements:
    print(f"--- Traitement de {equipement} ---")

    df_eq = df[df["equipement_id"] == equipement].copy()
    df_eq = df_eq.sort_values("timestamp")

    if len(df_eq) < 10:
        print(f"  Ignore: pas assez de donnees ({len(df_eq)} points)")
        continue

    client_id = df_eq["client_id"].iloc[0]

    # On predit chaque metrique separement, puis on les combine
    resultats_metriques = {}
    for colonne_source, config in METRIQUES.items():
        resultats_metriques[colonne_source] = predire_une_metrique(df_eq, colonne_source, config)

    # Combiner les 3 resultats (latence, debit, disponibilite) en un seul tableau
    # en les alignant sur la colonne "timestamp_predit"
    combine = resultats_metriques["latence_moyenne"]
    for colonne_source in ["debit_moyen", "disponibilite_moyenne"]:
        combine = combine.merge(
            resultats_metriques[colonne_source], on="timestamp_predit"
        )

    combine["equipement_id"] = equipement
    combine["client_id"] = client_id

    # Alerte globale : vraie si AU MOINS UNE des 3 metriques est en alerte
    combine["alerte_globale"] = (
        combine["alerte_latence_moyenne"]
        | combine["alerte_debit_moyen"]
        | combine["alerte_disponibilite_moyenne"]
    )

    nb_alertes = combine["alerte_globale"].sum()
    print(f"  30 predictions generees (3 metriques), {nb_alertes} alertes globales")

    toutes_les_predictions.append(combine)

# ============================================================
# 5. RASSEMBLER TOUS LES EQUIPEMENTS
# ============================================================
resultat_final = pd.concat(toutes_les_predictions, ignore_index=True)

resultat_final = resultat_final[[
    "client_id", "equipement_id", "timestamp_predit",
    "latence_predite", "alerte_latence_moyenne",
    "debit_predit", "alerte_debit_moyen",
    "disponibilite_predite", "alerte_disponibilite_moyenne",
    "alerte_globale"
]]

print(f"\n=== RESUME GLOBAL ===")
print(f"Total predictions: {len(resultat_final)}")
print(f"Total alertes globales: {resultat_final['alerte_globale'].sum()}")
print(f"  dont alertes latence: {resultat_final['alerte_latence_moyenne'].sum()}")
print(f"  dont alertes debit: {resultat_final['alerte_debit_moyen'].sum()}")
print(f"  dont alertes disponibilite: {resultat_final['alerte_disponibilite_moyenne'].sum()}")

# ============================================================
# 6. SAUVEGARDER DANS MINIO
# ============================================================
resultat_final.to_parquet(
    "s3://machinelearning/prophet_predictions.parquet",
    storage_options=STORAGE_OPTIONS
)

print(f"\nResultats sauvegardes dans s3://machinelearning/prophet_predictions.parquet")