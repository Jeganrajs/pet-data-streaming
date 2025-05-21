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
# url = "jdbc:postgresql://localhost:5432/online_orders"
pg_properties = {"user": pg_config['pg_user'],"password": pg_config['pg_pwd'],"driver": pg_config['pg_driver']}

def process_users_table():
    query = "(SELECT count(*) as loaded_time FROM public.users ) AS subquery"

    df = (spark.read 
        .format("jdbc")
        .option("driver",pg_config['pg_driver'])
        .option("url", pg_config['pg_url'])
        .option("dbtable", query)
        .option("user", pg_config['pg_user'])
        .option("password", pg_config['pg_pwd'])
        .load()
    )
    if df.isEmpty():
        users_df = spark.read.parquet(os.path.join(data_dir,"brz_online_users"))

        #  Save online users data into Postgres table
        print("Started saving resluts into postgres users table")
        users_df.write.jdbc(url=pg_config['pg_url'], table="users", mode="overwrite", properties=pg_properties)
        print("******** Reults saved into users table ************")
    else:
        print("Users table is already exist.. ")

def process_orders_table():
    """ Process orders table to postgress """

    orders_df = spark.read.parquet(os.path.join(data_dir,"brz_transactions"))
    # Get max timestamp from postgres table        
    query = "(SELECT max(loaded_time) as loaded_time FROM public.orders ) AS subquery"

    df = (spark.read 
        .format("jdbc")
        .option("driver",pg_config['pg_driver'])
        .option("url", pg_config['pg_url'])
        .option("dbtable", query)
        .option("user", pg_config['pg_user'])
        .option("password", pg_config['pg_pwd'])
        .load()
    )
    checkpoint_time = df.collect()[0][0]

    res_df = orders_df.where(f"to_timestamp(loaded_time) > to_timestamp('{checkpoint_time}') ")
    cnt = res_df.count()
    print(f"Number of records processed for orders table :: {cnt}")

    if cnt>0:
        # Save Transactions data into posgress table
        print("Started saving resluts into postgres Transactions table")
        res_df.write.jdbc(url=pg_config['pg_url'], table="orders", mode="append", properties=pg_properties)
        print("******** Reults saved into Transactions table ************")
    else:
        print("No new record to process for orders table...!")

if __name__ == "__main__":
    process_users_table()
    process_orders_table()
