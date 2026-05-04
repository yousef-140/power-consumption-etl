import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('AirFlowBatchProcessingJop') \
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

print("SparkConnectedSucceffly") 




schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("Rainfall", DoubleType(), True),
    StructField("pH", DoubleType(), True),
    StructField("EC", DoubleType(), True),
    StructField("Solar_Radiation", DoubleType(), True),
    StructField("Wind_Speed", DoubleType(), True),
    StructField("NDVI", DoubleType(), True),
    StructField("EVI", DoubleType(), True)
])

input_path = "file:///home/jovyan/work/data/raw_sensor_pings/"

print(f"Reading batch data from: {input_path}")

try:
    raw_df = spark.read \
        .schema(schema) \
        .json(input_path)


    record_count = raw_df.count()

    if record_count > 0:
        hdfs_output_path = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/sensor_data/"
        
        print(f" Processing {record_count} records...")
        print(f" Writing to HDFS (Bronze Layer): {hdfs_output_path}")
        
        # Save as Parquet - Industry standard for performance
        raw_df.write \
            .mode("append") \
            .format("parquet") \
            .save(hdfs_output_path)
        
        print("Batch ingestion complete. Data saved as Parquet.")
    else:
        print("No new data found in the landing zone.")

except Exception as e:
    print(f"Error during Spark processing: {e}")

finally:
  
    spark.stop()
    print("Spark Session Stopped.")