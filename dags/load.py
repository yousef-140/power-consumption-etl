import os

from pyspark.sql import SparkSession


def load():
    os.environ["HADOOP_USER_NAME"] = "root"

    spark = SparkSession.builder \
        .appName("Snowflake ETL") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
        .config(
            "spark.jars.packages",
            "net.snowflake:snowflake-jdbc:3.13.30,"
            "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4"
        ) \
        .getOrCreate()

    silver_df = spark.read.parquet(
        "hdfs://hadoop-namenode:9000/user/root/datalake/silver/power_consumption"
    )

    fact = silver_df.select(
        "event_timestamp",
        "Global_active_power",
        "Global_reactive_power",
        "Voltage",
        "Global_intensity",
        "Sub_metering_1",
        "Sub_metering_2",
        "Sub_metering_3",
        "total_sub_metering"
    )

    time_dim = silver_df.select(
        "event_timestamp",
        "hour",
        "day",
        "month",
        "year",
        "day_of_week",
        "is_weekend"
    ).dropDuplicates()

    fact.write.mode("overwrite").parquet(
        "hdfs://hadoop-namenode:9000/user/root/datalake/gold/fact_power"
    )

    time_dim.write.mode("overwrite").parquet(
        "hdfs://hadoop-namenode:9000/user/root/datalake/gold/time_dim"
    )

    snowflake_options = {
        "sfURL"      : "LK33768.eu-central-2.aws.snowflakecomputing.com",
        "sfUser"     : "Y0000sf",
        "sfPassword" : "YYoouusseeff12",
        "sfDatabase" : "POWER_CONSUMPTION_DB",
        "sfSchema"   : "GOLD_SCHEMA",
        "sfWarehouse": "POWER_EH"
    }

    time_dim.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "TIME_DIM") \
        .mode("overwrite") \
        .save()

    fact.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "FACT_POWER") \
        .mode("overwrite") \
        .save()


if __name__ == "__main__":
    load()

    
