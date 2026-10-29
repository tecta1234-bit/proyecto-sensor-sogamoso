from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count
from pyspark.sql.types import *

spark = SparkSession.builder.appName("MonitoreoSogamoso").getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","sensor_data") \
    .option("startingOffsets","earliest").load()

schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("temperatura", DoubleType()),
    StructField("humedad", DoubleType()),
    StructField("timestamp", StringType())
])

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Operaciones requeridas por la guía
alertas = parsed.filter(col("temperatura") > 35)
promedios = parsed.groupBy("sensor_id").agg(
    avg("temperatura").alias("temp_promedio"),
    count("*").alias("total_lecturas")
)

query = promedios.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
