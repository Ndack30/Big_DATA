from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, StructField
from pyspark.sql.functions import col, from_json, to_date
import os
from pyspark.sql.functions import when, lit
from pyspark.sql.functions import from_utc_timestamp
from pyspark.sql.functions import to_timestamp


# Initialiser SparkSession, inclusion des dépendances nécessaires pour Minio, kafka et spark-streaming
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.master", "local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.563") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Configuration pour Kafka
KAFKA_BROKER = "localhost:9092"
TOPIC = "transaction"

# Lire le flux Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Convertir les messages Kafka (JSON) en DataFrame structuré
schema = StructType([ \
    StructField("id_transaction", StringType()), \
    StructField("type_transaction", StringType()), \
    StructField("montant", DoubleType()), \
    StructField("devise", StringType()), \
    StructField("date", StringType()), \
    StructField("lieu", StringType()), \
    StructField("moyen_paiement", StringType())
])

#Permet de "rendre lisible" les données envoyées par KAFKA pour Spark + effectue les aggregats sur ces données directement
parsed_df = df.selectExpr("CAST(value AS STRING)") \
              .select(from_json(col("value"), schema).alias("data")) \
              .select("data.*") \
              .withColumn("montant", when(col("devise") == "USD", col("montant") * lit(0.85)).otherwise(col("montant"))
              ) \
              .withColumn("devise", when(col("devise") == "USD", lit("EUR")).otherwise(col("devise"))) \
              .withColumn("date_with_timezone", from_utc_timestamp(col("date"), "Europe/Paris")) \
              .filter(col("moyen_paiement") != "erreur") \
              .na.drop() \
              .withColumn("date", col("date").cast("date"))


parsed_df.printSchema()

# Afficher le flux traité
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Écriture dans MinIO en Parquet
query_minio = parsed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://warehouse/") \
    .option("checkpointLocation", "s3a://warehouse/") \
    .start()
#Partie optionnelle : Lire les fichiers parquet sur spark
def read_parquet_from_minio():
    # Lire les fichiers Parquet depuis MinIO
    parsed_df = spark.read.parquet("s3a://warehouse/")

    # Afficher le schéma du DataFrame
    parsed_df.printSchema()

    # Afficher les premières lignes du DataFrame
    parsed_df.show(10, truncate=False)

read_parquet_from_minio()

#Choix de laisser les deux query pour surveiller le fonctionnement
query.awaitTermination()
query_minio.awaitTermination()