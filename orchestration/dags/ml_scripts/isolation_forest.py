import pandas as pd
from sklearn.ensemble import IsolationForest

STORAGE_OPTIONS = {
    "key": "minioadmin",
    "secret": "minioadmin123",
    "client_kwargs": {"endpoint_url": "http://minio:9000"}
}


def executer_isolation_forest():
    """
    Charge les donnees Silver, entraine Isolation Forest,
    sauvegarde les resultats dans MinIO.
    """
    df = pd.read_parquet(
        "s3://silver/network-kpis-agrege/",
        storage_options=STORAGE_OPTIONS
    )
    print(f"Donnees chargees: {len(df)} lignes")

    colonnes_features = ["latence_moyenne", "debit_moyen", "disponibilite_moyenne"]
    X = df[colonnes_features]

    modele = IsolationForest(contamination=0.05, random_state=42)
    modele.fit(X)

    df["prediction"] = modele.predict(X)
    df["est_anomalie_predite"] = df["prediction"] == -1

    nb_anomalies = df["est_anomalie_predite"].sum()
    print(f"Anomalies detectees: {nb_anomalies} sur {len(df)} lignes")

    resultat_final = df[[
        "client_id", "equipement_id", "latence_moyenne", "debit_moyen",
        "disponibilite_moyenne", "nb_anomalies", "nb_mesures",
        "est_anomalie_predite"
    ]]

    resultat_final.to_parquet(
        "s3://machinelearning/isolation_forest_resultats.parquet",
        storage_options=STORAGE_OPTIONS
    )

    print("Resultats sauvegardes dans s3://machinelearning/isolation_forest_resultats.parquet")
    return nb_anomalies


if __name__ == "__main__":
    executer_isolation_forest()