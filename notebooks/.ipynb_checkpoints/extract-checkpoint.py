import os
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession


def extract():
    data = pd.read_csv(
        "/opt/airflow/data/source/household_power_consumption.txt",
        sep=";",
        low_memory=False
    )

    output_dir = Path("/opt/airflow/data/raw_power_batches")
    output_dir.mkdir(parents=True, exist_ok=True)

    BATCH_SIZE = 1000

    for start in range(0, len(data), BATCH_SIZE):
        end = start + BATCH_SIZE
        batch = data.iloc[start:end]

        batch_number = start // BATCH_SIZE + 1
        batch_file = output_dir / f"batch_{batch_number:05d}.csv"

        batch.to_csv(batch_file, index=False)

    os.environ["HADOOP_USER_NAME"] = "root"

    spark = SparkSession.builder \
        .appName("PowerConsumptionETL") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
        .getOrCreate()

    df = spark.read.csv(
        "file:///opt/airflow/data/raw_power_batches",
        header=True,
        inferSchema=True
    )

    hdfs_bronze_path = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/power_consumption"

    df.write \
        .mode("overwrite") \
        .parquet(hdfs_bronze_path)
