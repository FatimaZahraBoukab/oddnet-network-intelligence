import time
import random
import json
from datetime import datetime, timezone

# ============================================================
# 1. DÉFINITION DES CLIENTS ET DE LEURS ÉQUIPEMENTS
# ============================================================
# chaque client possède plusieurs équipements réseau.

clients = {
    "National_Telecom_Operator_MA": ["antenne_CASA_01", "antenne_RABAT_02"],
    "Mobile_Network_Operator_NA": ["routeur_FES_01", "routeur_TANGER_03"],
    "Pan_African_Bank": ["switch_AGADIR_01"],
    "Higher_Education_Network": ["antenne_MARRAKECH_01"]
}

# ============================================================
# 2. PLAGES DE VALEURS "NORMALES" (réalistes pour un réseau télécom)
# ============================================================
# Ces plages servent de référence : tant qu'on est dedans,
# c'est un fonctionnement normal. En dehors, c'est une anomalie.

LATENCE_NORMALE = (10, 50)       # en millisecondes (ms)
DEBIT_NORMAL = (50, 200)         # en Mbps
DISPONIBILITE_NORMALE = (98.0, 100.0)  # en pourcentage (%)

# Probabilité qu'une anomalie soit injectée à chaque mesure (5%)
PROBABILITE_ANOMALIE = 0.05


# ============================================================
# 3. FONCTION QUI GÉNÈRE UNE SEULE LIGNE DE DONNÉES
# ============================================================
def generer_mesure(client_id, equipement_id):
    """
    Génère une mesure réseau pour un équipement donné.
    Retourne un dictionnaire (qui sera converti en JSON).
    """

    # On décide d'abord si CETTE mesure sera une anomalie ou non
    est_anomalie = random.random() < PROBABILITE_ANOMALIE

    if est_anomalie:
        # --- Cas ANOMALIE : on génère des valeurs dégradées ---
        latence = round(random.uniform(150, 500), 2)      # latence très élevée
        debit = round(random.uniform(1, 20), 2)            # débit très faible
        disponibilite = round(random.uniform(60.0, 90.0), 2)  # dispo faible
    else:
        # --- Cas NORMAL : valeurs dans les plages habituelles ---
        latence = round(random.uniform(*LATENCE_NORMALE), 2)
        debit = round(random.uniform(*DEBIT_NORMAL), 2)
        disponibilite = round(random.uniform(*DISPONIBILITE_NORMALE), 2)

    # On construit le dictionnaire final (= future ligne de données)
    mesure = {
        "timestamp": datetime.now(timezone.utc).isoformat(),  # horodatage précis
        "client_id": client_id,
        "equipement_id": equipement_id,
        "latence_ms": latence,
        "debit_mbps": debit,
        "disponibilite_pct": disponibilite,
        "est_anomalie": est_anomalie   # utile plus tard pour valider Isolation Forest
    }

    return mesure


# ============================================================
# 4. BOUCLE PRINCIPALE : génère des mesures en continu
# ============================================================
def lancer_simulation(intervalle_secondes=3):
    """
    Boucle infinie qui génère une mesure pour chaque équipement,
    toutes les X secondes (par défaut 3s), et l'affiche en JSON.
    """
    print("=== Démarrage du simulateur ODDnet ===")
    print(f"Génération d'une mesure toutes les {intervalle_secondes}s. Ctrl+C pour arrêter.\n")

    try:
        while True:  # boucle infinie = flux continu
            for client_id, equipements in clients.items():
                for equipement_id in equipements:
                    mesure = generer_mesure(client_id, equipement_id)

                    # Pour l'instant on affiche juste en JSON dans le terminal.
                    # Plus tard (semaine 2), cette ligne sera remplacée par
                    # un envoi vers Kafka (producer.send(...))
                    print(json.dumps(mesure, ensure_ascii=False))

            time.sleep(intervalle_secondes)  # on attend avant le prochain tour

    except KeyboardInterrupt:
        # Permet d'arrêter proprement avec Ctrl+C, sans message d'erreur moche
        print("\n=== Simulation arrêtée manuellement ===")


# ============================================================
# 5. POINT D'ENTRÉE DU SCRIPT
# ============================================================
if __name__ == "__main__":
    # Ce bloc ne s'exécute QUE si on lance ce fichier directement
    # (pas si on l'importe depuis un autre script plus tard)
    lancer_simulation(intervalle_secondes=3)