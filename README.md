# 📡 ODDnet — Plateforme d'Intelligence Réseau

**Data Lakehouse & Machine Learning pour la détection d'anomalies et la maintenance prédictive des réseaux télécom**

---

## 🎯 Contexte

ODDnet est un intégrateur de systèmes télécoms opérant au Maroc et en Afrique, au service d'opérateurs mobiles, de fournisseurs d'accès internet et d'infrastructures télécom. Les données réseau étaient jusqu'ici gérées de façon fragmentée et manuelle (exports Excel, analyses ad hoc).

Ce projet met en place une **plateforme centralisée** capable de :
- Centraliser les flux de données réseau (latence, débit, disponibilité) en continu
- Détecter automatiquement les anomalies réseau via Machine Learning
- Prédire les pannes avant qu'elles ne surviennent
- Visualiser tout cela via des dashboards temps réel

---

## 🏗️ Architecture

```
Simulateur / Connecteur réel
        │
        ▼
   Apache Kafka (mode KRaft)
        │
        ▼
   Apache Spark Structured Streaming
        │
        ▼
   MinIO / Delta Lake  (Bronze → Silver → Gold via dbt)
        │
   ┌────┴─────┐
   ▼          ▼
 dbt/DuckDB   Machine Learning
 (Gold)       (Isolation Forest + Prophet)
   │               │
   └───────┬───────┘
           ▼
     PostgreSQL
           │
      ┌────┴────┐
      ▼         ▼
   Grafana   Streamlit

  Apache Airflow orchestre et surveille l'ensemble du pipeline
```

**Principe d'automatisation :** Docker Compose (`restart: always`) maintient les services continus en vie (Kafka, Spark Streaming, MinIO, Streamlit), tandis qu'Airflow orchestre les tâches planifiées (dbt, Machine Learning, surveillance).

---

## 🛠️ Stack technique

| Couche | Technologie | Rôle |
|---|---|---|
| Ingestion | Apache Kafka (KRaft) | Collecte des données réseau en streaming |
| Traitement | Apache Spark Structured Streaming | Agrégation par fenêtre temporelle |
| Stockage | MinIO + Delta Lake | Object storage S3-compatible, ACID |
| Transformation | dbt + DuckDB | Construction de la couche Gold |
| Orchestration | Apache Airflow | Planification et surveillance |
| Machine Learning | Isolation Forest + Prophet | Détection d'anomalies & prédiction de pannes |
| Visualisation | Grafana | Dashboards temps réel |
| Visualisation | Streamlit | Interface interactive (identité ODDnet) |
| Infrastructure | Docker Compose | Conteneurisation — 100% open source |

---

## 📁 Structure du projet

```
oddnet-pfa/
├── ingestion/            # Simulateur de données (producer Kafka)
├── processing/           # Job Spark Structured Streaming
├── transformation/       # Projet dbt + configuration DuckDB
│   ├── oddnet_transformation/
│   └── dbt_profiles/
├── ml/                   # Isolation Forest, Prophet, alimentation dashboard
├── orchestration/
│   └── dags/             # DAGs Airflow + scripts embarqués
│       ├── ml_scripts/
│       ├── dbt_project/
│       └── dbt_profiles/
├── dashboards/
│   └── streamlit/        # Application Streamlit
├── infra/                # docker-compose.yaml + Dockerfiles personnalisés
│   ├── airflow_docker/
│   ├── spark_docker/
│   └── (dbt_docker dans transformation/)

```

---

## 🚀 Démarrage rapide

### Installation

```bash
git clone https://github.com/FatimaZahraBoukab/oddnet-network-intelligence.git oddnet-pfa
cd oddnet-pfa

# Environnement Python
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Construction des images personnalisées
docker build -t oddnet-dbt ./transformation/dbt_docker
docker build -t oddnet-airflow ./infra/airflow_docker

# Démarrage de toute l'infrastructure
cd infra
docker compose up -d --build
```

Créer ensuite dans **MinIO** (`http://localhost:9001`) les buckets `bronze`, `silver`, `gold`, `machinelearning`, et dans **Kafka UI** (`http://localhost:8080`) le topic `network-kpis` (1 partition, 1 réplication).

### Lancer la génération de données

```bash
cd ingestion
python simulateur.py
```


## 🌐 Interfaces disponibles

| Interface | URL | Identifiants par défaut |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Spark Master | http://localhost:8081 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Airflow | http://localhost:8082 | `admin` / `admin` |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Streamlit | http://localhost:8501 | — |

> ⚠️ Ces identifiants sont destinés au développement local uniquement — à sécuriser avant tout déploiement en production.

---

## 🔄 Orchestration Airflow

| DAG | Fréquence | Rôle |
|---|---|---|
| `lancer_ml_oddnet` | Toutes les heures | `dbt_run` → `[isolation_forest, prophet_prediction]` → `alimenter_dashboard` |
| `verifier_pipeline_oddnet` | Toutes les 10 min | Vérifie la fraîcheur de la couche Silver |
| `verifier_gold_oddnet` | Toutes les 6h | Vérifie la fraîcheur de la couche Gold |

---

## 🤖 Machine Learning

- **Isolation Forest** : détection d'anomalies non supervisée sur latence/débit/disponibilité simultanément (`contamination=0.05`)
- **Prophet** : prédiction à 30 minutes, un modèle par équipement × par métrique, avec système d'alerte configurable par seuil


