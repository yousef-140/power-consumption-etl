# Power Consumption ETL Pipeline

A production-ready data engineering project that processes household power consumption data through a full ETL pipeline using Apache Spark, HDFS, Apache Airflow, Snowflake, and Power BI.

## Architecture

```
Raw Data → Apache Airflow → Apache Spark → HDFS Data Lake → Snowflake → Power BI
                                           (Bronze/Silver/Gold)
```

## Tech Stack

| Tool | Role |
|------|------|
| Apache Airflow | Pipeline orchestration |
| Apache Spark | Data processing |
| HDFS | Distributed data lake storage |
| Snowflake | Cloud data warehouse |
| Power BI | Analytics and dashboards |
| Docker | Containerization |

## Data Lake Layers

- **Bronze** — Raw data as ingested from source
- **Silver** — Cleaned and transformed data
- **Gold** — Analytics-ready fact and dimension tables

## Gold Layer Tables

- `FACT_POWER` — Power consumption measurements
- `TIME_DIM` — Time dimension with hour, day, month, year, weekend flag

## Pipeline Stages

### Extract
- Reads raw household power consumption data
- Splits into batches of 1000 rows
- Writes to HDFS bronze layer as Parquet

### Transform
- Cleans null values and invalid records
- Parses timestamps
- Engineers features: hour, day, month, year, day_of_week, is_weekend, peak_hours, total_sub_metering
- Writes to HDFS silver layer

### Load
- Builds fact and dimension tables from silver layer
- Writes to HDFS gold layer
- Loads into Snowflake for analytics

## Project Structure

```
├── dags/
│   ├── power_etl_dag.py       # Airflow DAG definition
│   ├── extract.py             # Extraction logic
│   ├── transform.py           # Transformation logic
│   └── load.py                # Loading logic
├── notebooks/                 # Development notebooks
├── data/                      # Data directory (source file not tracked)
├── jars/                      # Spark connector JARs
└── docker-compose.yaml        # Full infrastructure setup
```

## Infrastructure

All services run via Docker Compose:

- **Airflow** (webserver, scheduler, triggerer)
- **Spark** (via Jupyter PySpark notebook)
- **HDFS** (namenode + 2 datanodes)
- **YARN** (resource manager + 2 node managers)
- **PostgreSQL** (Airflow metadata database)

## Dashboard (Power BI)

- Average power consumption by month
- Power consumption by hour
- Weekend vs Weekday comparison
- Year slicer for filtering

## Dataset

[UCI Household Power Consumption Dataset](https://archive.ics.uci.edu/ml/datasets/individual+household+electric+power+consumption)

> Note: The raw data file is not tracked in this repository due to size (126MB). Download it separately and place it in `data/source/`.

## Author

Yousef — Data Engineering Project 2026
