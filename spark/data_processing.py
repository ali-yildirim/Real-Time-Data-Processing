from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging
import os
import sys

format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=format)
logger = logging.getLogger(__name__)

env = {
    "URI": os.getenv("URI"),
    "BOOTSTRAP_SERVERS": os.getenv("BOOTSTRAP_SERVERS"),
    "TOPIC": os.getenv("KAFKA_TOPIC"),
    "DATABASE":  os.getenv("DATABASE"),
    "COLLECTION": os.getenv("COLLECTION")
}


for key, value in env.items():
    if not value:
        logger.critical(f"Error: {key} environment variable must be set")
        logger.info("Exiting...")
        sys.exit(1)

spark = SparkSession.builder \
  .appName("KafkaExample") \
  .master("local[*]") \
  .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
  .config("spark.mongodb.output.uri", env["URI"]) \
  .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", env["BOOTSTRAP_SERVERS"]) \
  .option("subscribe", env["TOPIC"]) \
  .option("startingOffsets", "earliest") \
  .load()

df = df.selectExpr("CAST(value AS STRING)")

schema = StructType([
    StructField("uid", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("username", StringType(), True)
])

df = df.withColumn("value_schematic", from_json("value", schema)).select("value_schematic.*")

def write_to_mongo(df, epoch_id):
    record_count = df.count()
    print(f"Epoch {epoch_id} starting - received {record_count} records")
    
    if record_count > 0:        
        df.write \
          .format("mongo") \
          .option("database", env["DATABASE"]) \
          .option("collection", env["COLLECTION"]) \
          .mode("append") \
          .save()
        print(f"Epoch {epoch_id} completed - wrote {record_count} records to MongoDB")
    else:
        print(f"Epoch {epoch_id} - No data to write")

df.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoint") \
    .trigger(processingTime="10 seconds")  \
    .start() \
    .awaitTermination()