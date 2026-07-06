from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, sum as spark_sum
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType

# ============================================================
# 1. CRÉER LA SESSION SPARK
# ============================================================
# C'est le point d'entrée obligatoire pour utiliser Spark.
# On lui dit aussi de charger le connecteur Kafka nécessaire.

spark = SparkSession.builder \
    .appName("ODDnet_NetworkKPIs_Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")  # réduit les logs pour n'afficher que l'essentiel


# ============================================================
# 2. DÉFINIR LE SCHÉMA DES DONNÉES
# ============================================================
# Kafka transporte du JSON brut (juste du texte pour lui).
# Spark a besoin de savoir à quoi ressemble ce JSON pour le comprendre.
# Ce schéma doit correspondre EXACTEMENT à ce que génère simulateur.py

schema = StructType() \
    .add("timestamp", StringType()) \
    .add("client_id", StringType()) \
    .add("equipement_id", StringType()) \
    .add("latence_ms", DoubleType()) \
    .add("debit_mbps", DoubleType()) \
    .add("disponibilite_pct", DoubleType()) \
    .add("est_anomalie", BooleanType())


# ============================================================
# 3. LIRE LE FLUX KAFKA (readStream)
# ============================================================
# On se connecte au topic network-kpis. Notez l'adresse :
# "kafka:29092" et non "localhost:9092" — car Spark tourne
# DANS Docker, il doit utiliser l'adresse interne du réseau Docker
# (le listener PLAINTEXT_INTERNAL qu'on a configuré).

df_brut = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "network-kpis") \
    .option("startingOffsets", "latest") \
    .load()


# ============================================================
# 4. DÉCODER LE JSON
# ============================================================
# Kafka nous donne les messages sous forme de bytes bruts (colonne "value").
# On les convertit en texte, puis on les parse selon notre schéma.

df_json = df_brut.selectExpr("CAST(value AS STRING) as json_string")

df_parse = df_json.select(
    from_json(col("json_string"), schema).alias("data")
).select("data.*")   # "data.*" éclate les champs du JSON en colonnes séparées


# ============================================================
# 5. CONVERTIR LE TIMESTAMP TEXTE EN VRAI TIMESTAMP SPARK
# ============================================================
# Le timestamp arrive comme du texte ("2026-07-05T21:43:42...").
# Pour faire des fenêtres de temps, Spark a besoin d'un vrai type "timestamp".

from pyspark.sql.functions import to_timestamp

df_avec_ts = df_parse.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)


# ============================================================
# 6. AGRÉGATION PAR FENÊTRE DE TEMPS (le cœur du traitement)
# ============================================================
# On regroupe les données par fenêtres d'1 minute, PAR équipement,
# et on calcule des statistiques utiles.

resultat = df_avec_ts \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("client_id"),
        col("equipement_id")
    ) \
    .agg(
        avg("latence_ms").alias("latence_moyenne"),
        avg("debit_mbps").alias("debit_moyen"),
        avg("disponibilite_pct").alias("disponibilite_moyenne"),
        spark_sum(col("est_anomalie").cast("int")).alias("nb_anomalies"),
        count("*").alias("nb_mesures")
    )


# ============================================================
# 7. ÉCRIRE LE RÉSULTAT (writeStream) — mode console pour tester
# ============================================================
# Pour l'instant on affiche juste le résultat dans le terminal.

query = resultat.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()