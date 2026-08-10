from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime
import csv
import os

default_args = {
    "owner": "retailpulse",
    "retries": 1,
}

def read_gold_events(**context):
    from databricks import sql

    connection = sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("ACCESS_TOKEN")
    )
    cursor = connection.cursor()
    cursor.execute("""
                   SELECT order_month, category, order_count,
                   total_quantity, total_revenue
                   FROM retail_fresher.gold_monthly_category_sales
                   """)
    rows = cursor.fetchall()

    with open("/opt/airflow/kafka_scripts/data/monthly_category_sales.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sales_month", "category", "order_count", "total_quantity", "total_revenue"])
        writer.writerows(rows)

    cursor.close()
    connection.close()

with DAG(
    dag_id="retailpulse_weekly_orchestration",
    default_args=default_args,
    description="Weekly RetailPulse pipeline: Databricks -> Gold -> Kafka",
    schedule="0 9 * * 3",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retailpulse"]
) as dag:

    trigger_databricks_job = DatabricksRunNowOperator(
        task_id="trigger_databricks_job",
        databricks_conn_id="databricks_default",
        job_id=os.getenv("JOB_ID"),
        wait_for_termination=True
    )

    read_gold_events_task = PythonOperator(
        task_id="read_gold_events",
        python_callable=read_gold_events
    )

    publish_to_kafka_task = BashOperator(
        task_id="publish_to_kafka",
        bash_command="python /opt/airflow/kafka_scripts/producer.py"
    )

    consume_sample_task = BashOperator(
        task_id="consume_sample",
        bash_command="python /opt/airflow/kafka_scripts/consumer.py"
    )

    trigger_databricks_job >> read_gold_events_task >> publish_to_kafka_task >> consume_sample_task