from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if _name_ == "_main_":
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .getOrCreate()

    # Subscribe to a Kafka topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "music_activity") \
        .load()

    # Selecting and casting data as needed
    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # Query to print the results to the console
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()
