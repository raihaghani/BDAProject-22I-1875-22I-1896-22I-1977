from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, split

# Initialize Spark Session
spark = SparkSession.builder.appName("MusicRecommendation").getOrCreate()

# Load data
data_path = '/home/ebraheem/Documents/BDA_PROJECT/all_audio_features.csv'
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Handling string features with StringIndexer, handling null values
indexers = [
    StringIndexer(inputCol=column, outputCol=column + "_index", handleInvalid="skip") 
    for column in ["Artist", "Genre"]
]

# Pipeline for handling categorical transformations
pipeline = Pipeline(stages=indexers)
df_r = pipeline.fit(df).transform(df)

# Convert MFCC string to array of floats and create individual columns
df_r = df_r.withColumn("MFCC", split(col("MFCC"), ",\s*").cast("array<float>"))

# Create individual columns for each MFCC feature
for i in range(20):  # Adjust this number based on the actual MFCC features count
    df_r = df_r.withColumn(f"MFCC_{i}", df_r["MFCC"].getItem(i))

# Drop the original MFCC column after splitting
df_r = df_r.drop("MFCC")

# Assemble all features into a single vector, excluding the original categorical columns
feature_columns = [c for c in df_r.columns if c.endswith("index") or c.startswith("MFCC")]
feature_columns += ["Spectral Centroid", "Zero-Crossing Rate"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="skip")
final_data = assembler.transform(df_r)

# Show the transformed data to verify
final_data.select("features").show(truncate=False)

# Optional: Save the DataFrame to Parquet for further usage or model training
final_data.write.parquet("/home/ebraheem/Documents/BDA_PROJECT/Custom_Sample/processed_data.parquet")

# Stop the Spark session
spark.stop()