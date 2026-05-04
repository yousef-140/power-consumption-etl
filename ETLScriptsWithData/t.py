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

HDFS_BRONZE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/sensor_data/"
GOLD_BASE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"


def generate_static_date_dim(spark, start_date="2024-01-01", end_date="2030-12-31"):
    df = spark.sql(f"SELECT CAST('{start_date}' AS DATE) as start, CAST('{end_date}' AS DATE) as end")    
    df = df.select(
        F.explode(
            F.sequence(F.to_date("start"), F.to_date("end"), F.expr("interval 1 day"))
        ).alias("date")
    )

    dim_date = df.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").alias("day_of_week"),
        F.weekofyear("date").alias("week_of_year"),
        F.quarter("date").alias("quarter"),
        F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False).alias("is_weekend")
    )
    
    return dim_date



try:

    df_silver = spark.read.parquet(HDFS_BRONZE_PATH)
    
    df_silver = df_silver.withColumn("event_time", F.to_timestamp("event_time"))



    try:
        spark.read.parquet(f"{GOLD_BASE_PATH}dim_time")
        print("dim_date already exists. Skipping generation.")
        
    except Exception:
        print("dim_date not found. Generating static dimension...")
        static_date_table = generate_static_date_dim(spark)
        static_date_table.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_time")
    

    new_dim_sensor = df_silver.select("sensor_id").distinct() \
    .withColumn("sensor_type", F.lit("IoT-Agricultural-V1")) \
    .withColumn("firmware_version", F.lit("2.1.0"))

    try:
        existing_dim_sensor = spark.read.parquet(f"{GOLD_BASE_PATH}dim_sensor")
        final_dim_sensor = existing_dim_sensor.unionByName(new_dim_sensor) \
            .dropDuplicates(["sensor_id"])
        
        print("Merging new sensors into existing Master Dimension...")
    except Exception:
        print("Gold Dimension not found. Starting fresh.")
        final_dim_sensor = new_dim_sensor
    final_dim_sensor.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_sensor")

    fact_sensor_readings = df_silver \
        .withColumn("time_key", F.date_format("event_time", "yyyyMMddHHmmss")) \
        .withColumn("date_key", F.date_format("event_time", "yyyyMMdd").cast("int")) \
        .select(
            "time_key", 
            "sensor_id", 
            "Temperature", "Humidity", "Rainfall", "pH", 
            "EC", "Solar_Radiation", "Wind_Speed", "NDVI", "EVI","date_key"
        )

    print("Writing Star Schema tables to HDFS Gold Layer...")
    
  
    fact_sensor_readings.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}fact_sensor_readings")

    print("Gold Layer (Star Schema) created successfully.")

except Exception as e:
    print(f"Transformation failed: {e}")
    raise e

finally:
    spark.stop()


