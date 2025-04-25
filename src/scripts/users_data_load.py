import os
import sys
import time
import pandas
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from scripts.data_generator import create_user_data
from src.config.base_config import pg_config


spark = (
    SparkSession.builder.appName("Spark-Users-data-load-App")
    .config("spark.ui.showConsoleProgress", False)    
    .getOrCreate()
)

# cmd ==>  spark-submit  --packages org.postgresql:postgresql:42.7.5  /mnt/d/jegan/tmp_proj/src/scripts/users_data_load.py
# Set log level
spark.sparkContext.setLogLevel("ERROR")

# pg_conf = base_config.'pg_configs'

data_dir = "/mnt/d/jegan/prct/data"

users_data = create_user_data(100)
users_df = spark.createDataFrame(users_data)
res_df = users_df.selectExpr("*","to_date(created_time) as created_date")
res_df.show(3)

# url = "jdbc:postgresql://localhost:5432/postgres"
pg_properties = {"user": pg_config['pg_user'],"password": pg_config['pg_pwd'],"driver": pg_config['pg_driver']}
print(f"postgres cong : {pg_properties}")
res_df.write.jdbc(url=pg_config['pg_url'], table=pg_config["pg_user_table"], mode="overwrite", properties=pg_properties)
print("******** Reults saved into postgress table ************")
