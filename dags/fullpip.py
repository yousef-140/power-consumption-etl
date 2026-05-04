from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="power_consumption_etl_full_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id="extract",
        bash_command='docker exec -e PYTHONPATH=/usr/local/spark-3.5.0-bin-hadoop3/python:/usr/local/spark-3.5.0-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip spark-jupyter /opt/conda/bin/python /opt/airflow/dags/extract.py'
    )

    t2 = BashOperator(
        task_id="transform",
        bash_command='docker exec -e PYTHONPATH=/usr/local/spark-3.5.0-bin-hadoop3/python:/usr/local/spark-3.5.0-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip spark-jupyter /opt/conda/bin/python /opt/airflow/dags/transform.py'
    )

    t3 = BashOperator(
        task_id="load",
        bash_command='docker exec -e PYTHONPATH=/usr/local/spark-3.5.0-bin-hadoop3/python:/usr/local/spark-3.5.0-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip spark-jupyter /opt/conda/bin/python /opt/airflow/dags/load.py'
    )
    t1 >> t2 >> t3