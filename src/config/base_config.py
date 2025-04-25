import os
import sys 

# Postgres database related configs
pg_config = {"pg_schema":"public",
            "pg_user_table":"online_users",
            "pg_user":"postgres",
            "pg_pwd": os.environ['PG_PWD'],
            "pg_url":"jdbc:postgresql://localhost:5432/postgres",
            "pg_driver": "org.postgresql.Driver"
            }

# Config for spark data processer
spark_config = {
    "data_dir": "/mnt/d/jegan/prct/data/spark_stream_app",
    "raw_users_tbl":"brz_online_users"
}
