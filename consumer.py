import re
import findspark
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from datetime import datetime

if __name__ == "__main__":
    findspark.init()

    # Path to the pre-trained model
    path_to_model = 'pre_trained_model'  # Replace with the actual path to your pre-trained model

    # Config
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("RedditSentimentAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

    # Spark Context
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    # Schema for the incoming data
    schema = StructType([
        StructField("title", StringType()),
        StructField("content", StringType()),
        StructField("author", StringType()),
        StructField("upload_time", StringType())
    ])

    # Read the data from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "reddit_posts") \
        .option("startingOffsets", "latest") \
        .option("header", "true") \
        .load() \
        .selectExpr("CAST(value AS STRING) as message")

    df = df \
        .withColumn("value", from_json("message", schema)) \
        .select("value.title", "value.content", "value.author", "value.upload_time")

    # Pre-processing the data
    pre_process = udf(
        lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', x.lower().strip()).split(), ArrayType(StringType())
    )
    df = df.withColumn("cleaned_data", pre_process(df.content)).dropna()

    # Load the pre-trained model
    pipeline_model = PipelineModel.load(path_to_model)
    # Make predictions
    prediction = pipeline_model.transform(df)
    
    # Convert the Unix timestamp to a readable format
    convert_timestamp_udf = udf(lambda timestamp: datetime.fromtimestamp(float(timestamp)).strftime('%Y-%m-%d %H:%M:%S'),
                               StringType())
    prediction = prediction.withColumn("upload_time_readable", convert_timestamp_udf(prediction.upload_time))

    # Select the columns of interest
    prediction = prediction.select(prediction.title, prediction.author, prediction.upload_time_readable, prediction.prediction)

    # Print prediction in the console
    query = prediction \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()