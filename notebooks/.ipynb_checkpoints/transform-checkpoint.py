import os

from pyspark.sql import SparkSession, functions as F


def transform():
    os.environ["HADOOP_USER_NAME"] = "root"

    spark = SparkSession.builder \
        .appName("transformation") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
        .getOrCreate()

    bronze_path = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/power_consumption"
    bronze_df = spark.read.parquet(bronze_path)

    silver_df = bronze_df.fillna({"Sub_metering_3": 0})

    silver_df = silver_df.withColumn(
        "Time_fixed",
        F.date_format(F.col("time"), "HH:mm:ss")
    )

    silver_df = silver_df.withColumn(
        "datetime_str",
        F.concat_ws(" ", F.col("Date"), F.col("Time_fixed"))
    )

    silver_df = silver_df.withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("datetime_str"), "d/M/yyyy HH:mm:ss")
    )

    silver_df = silver_df.replace("?", None)

    numeric_cols = [
        "Global_active_power",
        "Global_reactive_power",
        "Voltage",
        "Global_intensity",
        "Sub_metering_1",
        "Sub_metering_2",
        "Sub_metering_3"
    ]

    for col in numeric_cols:
        silver_df = silver_df.withColumn(col, F.col(col).cast("double"))

    silver_df = silver_df.dropna(subset=[
        "Global_active_power",
        "Global_reactive_power",
        "Voltage",
        "Global_intensity",
        "Sub_metering_1",
        "Sub_metering_2",
        "Sub_metering_3",
        "event_timestamp"
    ])

    silver_df = silver_df \
        .withColumn("hour", F.hour("event_timestamp")) \
        .withColumn("day", F.dayofmonth("event_timestamp")) \
        .withColumn("month", F.month("event_timestamp")) \
        .withColumn("year", F.year("event_timestamp")) \
        .withColumn("day_of_week", F.dayofweek("event_timestamp"))

    silver_df = silver_df.withColumn(
        "total_sub_metering",
        F.col("Sub_metering_1") + F.col("Sub_metering_2") + F.col("Sub_metering_3")
    )

    silver_df = silver_df.drop("Date", "Time", "Time_fixed", "datetime_str")

    silver_df = silver_df.withColumn(
        "is_weekend",
        F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0)
    )

    silver_df = silver_df.withColumn(
        "peak_hours",
        F.when((F.col("hour") >= 18) & (F.col("hour") <= 23), 1).otherwise(0)
    )

    silver_path = "hdfs://hadoop-namenode:9000/user/root/datalake/silver/power_consumption"

    silver_df.write \
        .mode("overwrite") \
        .parquet(silver_path)
