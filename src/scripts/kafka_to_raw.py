import os, sys 
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.window import Window
from src.config.base_config import spark_config

data_dir = spark_config['data_dir']
checkpoint_dir = os.path.join(data_dir,"src_to_raw_checkpoint")

tbl_path = os.path.join(data_dir,"brz_users_data")

# CMD >> spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0  /mnt/d/jegan/tmp_proj/src/scripts/kafka_to_raw.py

spark = (
    SparkSession.builder.appName("KafkaToRawApp")
    .config("spark.ui.showConsoleProgress", False)    
    .getOrCreate()
) 


src_df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "OnlineUsersTopic")
    .load()
)

# Example: Convert binary key and value to string
src_df = src_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Example: Parse JSON data from the value column
# df = df.selectExpr("CAST(value AS STRING)", "from_json(value, schema) as data")

query = ( src_df
    .writeStream
    .format("parquet")  # Or your desired output format (e.g., "parquet", "delta")
    # .trigger(continuous='1 second')
    .option("checkpointLocation", checkpoint_dir)  # For fault tolerance
    .option("path",(os.path.join(data_dir,spark_config["raw_users_tbl"]) ))
    .start()
)

query.awaitTermination()