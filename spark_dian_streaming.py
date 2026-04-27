from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StringType, IntegerType, LongType

spark = SparkSession.builder \
    .appName("DIANStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("emisor", StringType()) \
    .add("tipo_documento", StringType()) \
    .add("valor", IntegerType()) \
    .add("timestamp", LongType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "dian_stream") \
    .load()

df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_final = df_parsed \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("emisor"),
        col("tipo_documento")
    ).count()

query = df_final.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()