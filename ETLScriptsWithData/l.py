import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType


os.environ["HADOOP_USER_NAME"] = "root"


spark = SparkSession.builder \
    .appName('starSchemaTrasnformations') \
    .master('yarn') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.30.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m") \
    .getOrCreate()


sf_options = {
    "sfURL": "LLYHYMM-YJ95431.snowflakecomputing.com",
    "sfUser": "YOURUSERNAME",
    "sfPassword": "YOURPASSWORD",
    "sfDatabase": "AGRI_DATA_DB",
    "sfSchema": "GOLD_LAYER",
    "sfWarehouse": "AGRI_WH"
}

GOLD_BASE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"

def check_table_exists(spark, table_name):
    target_table = table_name.upper()
    
    check_query = f"""
    SELECT TABLE_NAME 
    FROM AGRI_DATA_DB.INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'GOLD_LAYER' 
    AND TABLE_NAME = '{target_table}'
    """
    
    exists_df = spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("query", check_query) \
        .load()

    return exists_df.count() > 0



def load_to_snowflake(table_name):

    if table_name.lower() == "dim_time":
        try:
            exist =check_table_exists(spark,table_name)
            if exist :
                print(f"--- SKIPPING {table_name}: Table already exist ---")
                return 
        except Exception:
            print(f"{table_name} doesn't exist yet. Proceeding with initial load.")
    
    temp_table = f"AGRI_DATA_DB.GOLD_LAYER.{table_name.upper()}_TEMP"
    final_table = f"AGRI_DATA_DB.GOLD_LAYER.{table_name.upper()}"

    print(f"--- Starting Atomic Load for {final_table} ---")
    
    print(f"Reading {table_name} from HDFS...")
    df = spark.read.parquet(f"{GOLD_BASE_PATH}{table_name}")
    
    print(f"Loading data to temporary table: {temp_table}...")
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", temp_table) \
        .mode("overwrite") \
        .save()
    
    print(f"Swapping {temp_table} with {final_table}...")
    
    try:
        snowflake_utils = spark._jvm.net.snowflake.spark.snowflake.Utils
        swap_query = f"ALTER TABLE IF EXISTS {final_table} SWAP WITH {temp_table}"
        snowflake_utils.runQuery(sf_options, swap_query)
        print(f"Cleaning up temporary table...")
        drop_query = f"DROP TABLE IF EXISTS {temp_table}"
        snowflake_utils.runQuery(sf_options, drop_query)
        
        print(f"SUCCESS: {final_table} loaded atomically.")

    except Exception as e:
        print(f"ERROR during Atomic Swap for {final_table}: {e}")
        raise e 





def load_fact_to_snowflake(table_name):
    target_table = table_name.upper()
    
    print(f"--- Starting Incremental Append ")
    df = spark.read.parquet(f"{GOLD_BASE_PATH}{table_name}")
    print(f"Appending new records")
    try:
        df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**sf_options) \
            .option("dbtable", target_table) \
            .mode("append") \
            .save()
        print(f"SUCCESS: {target_table} appended successfully.")
    except Exception as e:
        print(f"APPEND FAILED for {target_table}: {e}")
        raise e  


try:
    load_to_snowflake("dim_time")
    load_to_snowflake("dim_sensor")
    load_fact_to_snowflake("fact_sensor_readings")
    print("ALL GOLD TABLES LOADED TO SNOWFLAKE")
except Exception as e:
    print(f" Loading failed: {e}")
    raise e
finally:
    spark.stop()