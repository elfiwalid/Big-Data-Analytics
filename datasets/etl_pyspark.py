from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, round

# Créer la session Spark
spark = SparkSession.builder \
    .appName("NYC Yellow Taxi ETL") \
    .master("local[*]") \
    .getOrCreate()

# Lire le fichier parquet
df = spark.read.parquet("/datasets/yellow_tripdata_2024-01.parquet")

# Nettoyer et enrichir les données
df_clean = (df
    .filter((col("fare_amount") > 1) & (col("trip_distance") > 0.5))
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    .withColumn("dow", dayofweek(col("tpep_pickup_datetime")))
    .withColumn("fare_per_km", round(col("fare_amount") / col("trip_distance"), 2))
)

# Sauvegarder le résultat nettoyé
df_clean.select(
    "tpep_pickup_datetime", "pickup_hour", "dow",
    "PULocationID", "DOLocationID",
    "trip_distance", "fare_amount", "fare_per_km"
).write.mode("overwrite").parquet("/datasets/ytaxi_2024_01_clean.parquet")

print("✅ Nettoyage terminé et fichier sauvegardé !")

spark.stop()

