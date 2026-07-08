from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, sum as spark_sum, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType

# ============================================================
# 1. CRÉER LA SESSION SPARK — avec support MinIO (S3) + Delta Lake
# ============================================================

spark = SparkSession.builder \
    .appName("ODDnet_NetworkKPIs_Streaming") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. DÉFINIR LE SCHÉMA DES DONNÉES
# ============================================================
# Doit correspondre exactement à ce que génère simulateur.py

schema = StructType() \
    .add("timestamp", StringType()) \
    .add("client_id", StringType()) \
    .add("equipement_id", StringType()) \
    .add("latence_ms", DoubleType()) \
    .add("debit_mbps", DoubleType()) \
    .add("disponibilite_pct", DoubleType()) \
    .add("est_anomalie", BooleanType())


# ============================================================
# 3. LIRE LE FLUX KAFKA
# ============================================================
# Adresse interne Docker : kafka:29092 (pas localhost)

df_brut = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "network-kpis") \
    .option("startingOffsets", "latest") \
    .load()


# ============================================================
# 4. DÉCODER LE JSON
# ============================================================

df_json = df_brut.selectExpr("CAST(value AS STRING) as json_string")

df_parse = df_json.select(
    from_json(col("json_string"), schema).alias("data")
).select("data.*")


# ============================================================
# 5. CONVERTIR LE TIMESTAMP TEXTE EN VRAI TIMESTAMP SPARK
# ============================================================

df_avec_ts = df_parse.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)


# ============================================================
# 6. ÉCRITURE BRONZE — données brutes, sans transformation
# ============================================================

query_bronze = df_avec_ts.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("path", "s3a://bronze/network-kpis-raw") \
    .option("checkpointLocation", "s3a://bronze/_checkpoints/network-kpis-raw") \
    .trigger(processingTime="30 seconds") \
    .start()


# ============================================================
# 7. AGRÉGATION PAR FENÊTRE DE TEMPS (1 minute, par équipement)
# ============================================================

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
# 8. ÉCRITURE SILVER — données agrégées
# ============================================================

query_silver = resultat.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("path", "s3a://silver/network-kpis-agrege") \
    .option("checkpointLocation", "s3a://silver/_checkpoints/network-kpis-agrege") \
    .trigger(processingTime="30 seconds") \
    .start()


# ============================================================
# 9. ATTENDRE LES DEUX FLUX EN PARALLÈLE
# ============================================================

spark.streams.awaitAnyTermination()