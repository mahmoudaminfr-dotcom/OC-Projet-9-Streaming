from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# Initialisation de la session Spark avec l'hôte UI forcé
spark = SparkSession.builder \
    .appName("TicketProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.ui.host", "0.0.0.0") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Définition du schéma des tickets JSON
schema = StructType([
    StructField("ticket_id", IntegerType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("ticket_type", StringType(), True),
    StructField("team_assigned", StringType(), True),
    StructField("status", StringType(), True)
])

print("Démarrage du flux de lecture PySpark depuis Redpanda...")

# Lecture du flux Kafka/Redpanda
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda_broker:9092") \
    .option("subscribe", "client_tickets") \
    .option("startingOffsets", "earliest") \
    .load()

# Transformation des valeurs binaires en JSON structuré
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Agrégation : Comptage des tickets par équipe
agg_df = parsed_df.groupBy("team_assigned").count()

# Export 1 : Affichage des agrégations dans la console
console_query = agg_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

# Export 2 : Sauvegarde des données brutes au format Parquet
parquet_query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "/app/output/tickets_parquet") \
    .option("checkpointLocation", "/app/output/checkpoints") \
    .outputMode("append") \
    .start()

# Maintien de l'exécution
spark.streams.awaitAnyTermination()
