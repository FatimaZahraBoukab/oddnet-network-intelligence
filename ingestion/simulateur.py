import time
import random
import json
from datetime import datetime, timezone
from kafka import KafkaProducer  # NOUVEAU : pour parler à Kafka

# ============================================================
# 1. DÉFINITION DES CLIENTS ET DE LEURS ÉQUIPEMENTS
# ============================================================
clients = {
    "National_Telecom_Operator_MA": ["antenne_CASA_01", "antenne_RABAT_02"],
    "Mobile_Network_Operator_NA": ["routeur_FES_01", "routeur_TANGER_03"],
    "Pan_African_Bank": ["switch_AGADIR_01"],
    "Higher_Education_Network": ["antenne_MARRAKECH_01"]
}

# ============================================================
# 2. PLAGES DE VALEURS "NORMALES"
# ============================================================
LATENCE_NORMALE = (10, 50)
DEBIT_NORMAL = (50, 200)
DISPONIBILITE_NORMALE = (98.0, 100.0)
PROBABILITE_ANOMALIE = 0.05

# ============================================================
# 3. NOUVEAU : CONFIGURATION DU PRODUCER KAFKA
# ============================================================
# On crée le producer UNE SEULE FOIS, en dehors de la boucle.
# C'est lui qui va envoyer chaque mesure vers Kafka.

NOM_TOPIC = "network-kpis"  # doit correspondre exactement au topic créé dans Kafka UI

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',   # adresse de Kafka (celle accessible depuis ton PC)
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # dict Python → JSON → bytes
)


# ============================================================
# 4. FONCTION QUI GÉNÈRE UNE SEULE LIGNE DE DONNÉES (inchangée)
# ============================================================
def generer_mesure(client_id, equipement_id):
    est_anomalie = random.random() < PROBABILITE_ANOMALIE

    if est_anomalie:
        latence = round(random.uniform(150, 500), 2)
        debit = round(random.uniform(1, 20), 2)
        disponibilite = round(random.uniform(60.0, 90.0), 2)
    else:
        latence = round(random.uniform(*LATENCE_NORMALE), 2)
        debit = round(random.uniform(*DEBIT_NORMAL), 2)
        disponibilite = round(random.uniform(*DISPONIBILITE_NORMALE), 2)

    mesure = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "client_id": client_id,
        "equipement_id": equipement_id,
        "latence_ms": latence,
        "debit_mbps": debit,
        "disponibilite_pct": disponibilite,
        "est_anomalie": est_anomalie
    }

    return mesure


# ============================================================
# 5. BOUCLE PRINCIPALE — MODIFIÉE pour envoyer à Kafka
# ============================================================
def lancer_simulation(intervalle_secondes=3):
    print("=== Démarrage du simulateur ODDnet (mode Kafka) ===")
    print(f"Envoi vers le topic '{NOM_TOPIC}' toutes les {intervalle_secondes}s. Ctrl+C pour arrêter.\n")

    try:
        while True:
            for client_id, equipements in clients.items():
                for equipement_id in equipements:
                    mesure = generer_mesure(client_id, equipement_id)

                    # NOUVEAU : on envoie la mesure à Kafka au lieu de juste l'afficher
                    producer.send(NOM_TOPIC, value=mesure)

                    # On garde un affichage simple pour suivre visuellement ce qui est envoyé
                    print(f"[ENVOYÉ] {mesure['client_id']} | {mesure['equipement_id']} | "
                          f"latence={mesure['latence_ms']}ms | anomalie={mesure['est_anomalie']}")

            # NOUVEAU : force l'envoi immédiat des messages en attente
            producer.flush()

            time.sleep(intervalle_secondes)

    except KeyboardInterrupt:
        print("\n=== Simulation arrêtée manuellement ===")
        producer.close()  # NOUVEAU : ferme proprement la connexion à Kafka


# ============================================================
# 6. POINT D'ENTRÉE DU SCRIPT
# ============================================================
if __name__ == "__main__":
    lancer_simulation(intervalle_secondes=3)