import os
import sys
import time
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config.base_config import pg_config,spark_config


spark = (
    SparkSession.builder.appName("Spark-Users-data-load-App")
    .config("spark.ui.showConsoleProgress", False)    
    .getOrCreate()
)
# Set log level
spark.sparkContext.setLogLevel("ERROR")

# >> spark-submit  --packages org.postgresql:postgresql:42.7.5  /mnt/d/jegan/git_repos/pet-data-streaming/src/data_processor/r2p_orders.py

data_dir = spark_config["data_dir"]
checkpoint_loc = os.path.join(data_dir,"checkpoints","r2p_orders")

users_df = spark.read.parquet(os.path.join(data_dir,"brz_online_users"))
orders_df = spark.read.parquet(os.path.join(data_dir,"brz_transactions"))
res_df = orders_df.where("loaded_time = '2025-05-19 19:03:26' ")

#  Save online users data into Postgres table
url = "jdbc:postgresql://localhost:5432/online_orders"
pg_properties = {"user": pg_config['pg_user'],"password": pg_config['pg_pwd'],"driver": pg_config['pg_driver']}
# print(f"postgres cong : {pg_properties}")
print("Started saving resluts into postgres users table")
# users_df.write.jdbc(url=pg_config['pg_url'], table="users", mode="overwrite", properties=pg_properties)
print("******** Reults saved into users table ************")

# Save Transactions data into posgress table
print("Started saving resluts into postgres Transactions table")
res_df.write.jdbc(url=pg_config['pg_url'], table="orders", mode="append", properties=pg_properties)
print("******** Reults saved into Transactions table ************")

