from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, col, when

spark = SparkSession.builder.appName("NYC Taxi ETL").getOrCreate()
df = spark.read.parquet("/home/walid1234/data_engineering/datasets/yellow_tripdata_2024-01.parquet")

# Enrichir
df = df.withColumn('trip_duration_min',
                   (unix_timestamp('tpep_dropoff_datetime') - unix_timestamp('tpep_pickup_datetime')) / 60)
df = df.withColumn('avg_speed_kmh',
                   (col('trip_distance') / (col('trip_duration_min') / 60)))
df = df.withColumn('fare_category',
                   when(col('fare_amount') > 50, 'high_fare').otherwise('normal'))

# Nettoyer
df = df.filter((col('trip_duration_min') > 0) &
               (col('avg_speed_kmh') < 200) &
               (col('trip_distance') < 200))

# Sauver
df.write.mode("overwrite").parquet("/home/walid1234/data_engineering/datasets/ytaxi_2024_01_clean.parquet")

