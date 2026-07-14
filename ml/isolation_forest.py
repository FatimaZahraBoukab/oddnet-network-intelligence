import pandas as pd
from sklearn.ensemble import IsolationForest

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

print(f"Donnees chargees: {len(df)} lignes")

# ============================================================
# 2. SÉLECTIONNER LES COLONNES UTILES POUR LE MODÈLE
# ============================================================
# Isolation Forest a besoin de colonnes NUMERIQUES uniquement.
# On ignore "window" (dictionnaire), "client_id"/"equipement_id" (texte).

colonnes_features = ["latence_moyenne", "debit_moyen", "disponibilite_moyenne"]

X = df[colonnes_features]

print(f"\nColonnes utilisees pour l'entrainement: {colonnes_features}")
print(X.describe())

# ============================================================
# 3. ENTRAÎNER ISOLATION FOREST
# ============================================================
# contamination = proportion attendue d'anomalies dans les donnees
# (on sait que le simulateur injecte environ 5% d'anomalies)

modele = IsolationForest(
    contamination=0.05,
    random_state=42
)

modele.fit(X)

# ============================================================
# 4. APPLIQUER LE MODÈLE : PRÉDIRE NORMAL / ANOMALIE
# ============================================================
# predict() retourne : 1 = normal, -1 = anomalie

df["prediction"] = modele.predict(X)
df["est_anomalie_predite"] = df["prediction"] == -1

nb_anomalies_detectees = df["est_anomalie_predite"].sum()
print(f"\nAnomalies detectees par le modele: {nb_anomalies_detectees} sur {len(df)} lignes")
print(f"Pourcentage: {nb_anomalies_detectees / len(df) * 100:.1f}%")

# ============================================================
# 5. VALIDATION : COMPARER AVEC nb_anomalies (deja connu)
# ============================================================
# On regarde si les lignes detectees comme anomalies par le modele
# correspondent bien aux fenetres qui avaient deja des nb_anomalies eleves

print("\n--- Comparaison ---")
print("Moyenne de nb_anomalies pour les lignes jugees NORMALES par le modele:")
print(df[~df["est_anomalie_predite"]]["nb_anomalies"].mean())

print("\nMoyenne de nb_anomalies pour les lignes jugees ANORMALES par le modele:")
print(df[df["est_anomalie_predite"]]["nb_anomalies"].mean())

# ============================================================
# 6. APERÇU DES ANOMALIES DÉTECTÉES
# ============================================================
print("\n--- Exemples d'anomalies detectees ---")
print(df[df["est_anomalie_predite"]][
    ["client_id", "equipement_id", "latence_moyenne", "debit_moyen",
     "disponibilite_moyenne", "nb_anomalies"]
].head(10)) 

# ============================================================
# 7. SAUVEGARDER LE RÉSULTAT DANS MINIO (bucket "ml")
# ============================================================
resultat_final = df[[
    "client_id", "equipement_id", "latence_moyenne", "debit_moyen",
    "disponibilite_moyenne", "nb_anomalies", "nb_mesures",
    "est_anomalie_predite"
]]

resultat_final.to_parquet(
    "s3://machinelearning/isolation_forest_resultats.parquet",
    storage_options=STORAGE_OPTIONS
)

print(f"\nResultats sauvegardes dans s3://machinelearning/isolation_forest_resultats.parquet")
print(f"({len(resultat_final)} lignes)")