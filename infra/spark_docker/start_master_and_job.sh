#!/bin/bash

# Demarrer le vrai processus Master de Spark en arriere-plan
/opt/bitnami/scripts/spark/run.sh &

# Attendre que Kafka/MinIO soient prets
sleep 20

# Lancer le job de streaming en arriere-plan
spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.0.0 \
  /opt/processing/spark_streaming.py &

# Attendre indefiniment (garde le container actif)
wait
