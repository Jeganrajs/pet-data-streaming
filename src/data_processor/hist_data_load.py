import os
import sys
import time
import pandas as pd
from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
from data_gen.data_generator import create_user_data
from data_gen.trans_data_generator import TransactionFakerModel
from src.config.base_config import pg_config,spark_config


# spark = (
#     SparkSession.builder.appName("Spark-Users-data-load-App")
#     .config("spark.ui.showConsoleProgress", False)    
#     .getOrCreate()
# )

data_type = sys.argv[1]
# >> spark-submit  --packages org.postgresql:postgresql:42.7.5  /mnt/d/jegan/git_repos/pet-data-streaming/src/scripts/hist_data_load.py  transactions_data
# Set log level
# spark.sparkContext.setLogLevel("ERROR")

data_dir = spark_config["data_dir"]

if data_type == "users_data":
    print("Generating Users data..!")
    users_data = create_user_data(1000000)
    # users_df = spark.createDataFrame(users_data)
    # res_df = users_df.selectExpr("*","to_date(current_timestamp()) as created_date")
    # res_df.show(3)
    # res_df.write.mode("OVERWRITE").parquet(os.path.join(data_dir,"brz_online_users"))    
    print("********* Users data saved as parquet file *********")
elif data_type == "transactions_data":
    st_time = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Generating Transactions data.. as {st_time}")
    transaction_faker = TransactionFakerModel()
    trans_data = transaction_faker.generate_transactions(500000)
    # print(trans_data)
    trans_df = pd.DataFrame(trans_data)    
    trans_df["loaded_time"] = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    # res_df = trans_df.selectExpr("*","to_date(current_timestamp()) as created_date","current_timestamp() as loaded_time")
    # res_df.show(3)
    # res_df.write.mode("APPEND").parquet(os.path.join(data_dir,"brz_transactions"))
    file_suffix = datetime.today().strftime("%Y%m%d%H%M%S")
    trans_df.to_parquet(os.path.join(data_dir,"brz_transactions",f"transaction_{file_suffix}"))
    end_time = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    print(f"********* Transactions data saved as parquet file at {end_time} *********")


# /////////////////////////////////////////
#  Save online users data into Postgres table
# url = "jdbc:postgresql://localhost:5432/postgres"
# pg_properties = {"user": pg_config['pg_user'],"password": pg_config['pg_pwd'],"driver": pg_config['pg_driver']}
# print(f"postgres cong : {pg_properties}")
# res_df.write.jdbc(url=pg_config['pg_url'], table=pg_config["pg_user_table"], mode="overwrite", properties=pg_properties)
# print("******** Reults saved into postgress table ************")
# /////////////////////////////////////////
