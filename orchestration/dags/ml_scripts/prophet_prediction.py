import pandas as pd
from prophet import Prophet
import logging

logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

STORAGE_OPTIONS = {
    "key": "minioadmin",
    "secret": "minioadmin123",
    "client_kwargs": {"endpoint_url": "http://minio:9000"}
}

METRIQUES = {
    "latence_moyenne": {
        "seuil": 100,
        "sens": "au_dessus",
        "nom_colonne_predite": "latence_predite"
    },
    "debit_moyen": {
        "seuil": 50,
        "sens": "en_dessous",
        "nom_colonne_predite": "debit_predit"
    },
    "disponibilite_moyenne": {
        "seuil": 95,
        "sens": "en_dessous",
        "nom_colonne_predite": "disponibilite_predite"
    }
}


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

    if config["sens"] == "au_dessus":
        predictions_futures["alerte_" + colonne_source] = (
            predictions_futures[config["nom_colonne_predite"]] > config["seuil"]
        )
    else:
        predictions_futures["alerte_" + colonne_source] = (
            predictions_futures[config["nom_colonne_predite"]] < config["seuil"]
        )

    return predictions_futures


def executer_prophet():
    """
    Charge les donnees Silver, entraine Prophet pour chaque equipement
    et chaque metrique (latence, debit, disponibilite),
    sauvegarde les resultats dans MinIO.
    """
    df = pd.read_parquet(
        "s3://silver/network-kpis-agrege/",
        storage_options=STORAGE_OPTIONS
    )

    df["timestamp"] = df["window"].apply(lambda w: w["start"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    print(f"Donnees chargees: {len(df)} lignes")

    equipements = df["equipement_id"].unique()
    print(f"Equipements a traiter: {list(equipements)}")

    toutes_les_predictions = []

    for equipement in equipements:
        print(f"--- Traitement de {equipement} ---")

        df_eq = df[df["equipement_id"] == equipement].copy()
        df_eq = df_eq.sort_values("timestamp")

        if len(df_eq) < 10:
            print(f"  Ignore: pas assez de donnees ({len(df_eq)} points)")
            continue

        client_id = df_eq["client_id"].iloc[0]

        resultats_metriques = {}
        for colonne_source, config in METRIQUES.items():
            resultats_metriques[colonne_source] = predire_une_metrique(df_eq, colonne_source, config)

        combine = resultats_metriques["latence_moyenne"]
        for colonne_source in ["debit_moyen", "disponibilite_moyenne"]:
            combine = combine.merge(
                resultats_metriques[colonne_source], on="timestamp_predit"
            )

        combine["equipement_id"] = equipement
        combine["client_id"] = client_id

        combine["alerte_globale"] = (
            combine["alerte_latence_moyenne"]
            | combine["alerte_debit_moyen"]
            | combine["alerte_disponibilite_moyenne"]
        )

        nb_alertes = combine["alerte_globale"].sum()
        print(f"  30 predictions generees (3 metriques), {nb_alertes} alertes globales")

        toutes_les_predictions.append(combine)

    resultat_final = pd.concat(toutes_les_predictions, ignore_index=True)

    resultat_final = resultat_final[[
        "client_id", "equipement_id", "timestamp_predit",
        "latence_predite", "alerte_latence_moyenne",
        "debit_predit", "alerte_debit_moyen",
        "disponibilite_predite", "alerte_disponibilite_moyenne",
        "alerte_globale"
    ]]

    total_alertes = resultat_final["alerte_globale"].sum()
    print(f"Total predictions: {len(resultat_final)}")
    print(f"Total alertes globales: {total_alertes}")

    resultat_final.to_parquet(
        "s3://machinelearning/prophet_predictions.parquet",
        storage_options=STORAGE_OPTIONS
    )

    print("Resultats sauvegardes dans s3://machinelearning/prophet_predictions.parquet")
    return total_alertes


if __name__ == "__main__":
    executer_prophet()