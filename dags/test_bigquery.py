from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
from lib.utils import *

dag_name = "TEST_BIGQUERY"

sql, conf, sa_key, params = get_params(dag_name)
project = params["project"]
location = params["location"]

# Harmless Python functions for each task
def task_one():
    print("✅ Task 1 completed")

def task_two():
    print("✅ Task 2 completed")

def task_three():
    print("✅ Task 3 completed")

# Define the DAG
dag = DAG(
    dag_id=dag_name,
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Run manually
    catchup=False,
    tags=["test"]
)

    t1 = PythonOperator(
        task_id="task_one",
        python_callable=task_one,
    )

    t2 = PythonOperator(
        task_id="task_two",
        python_callable=task_two,
    )

    t3 = PythonOperator(
        task_id="task_three",
        python_callable=task_three,
    )

    bq_task = BigQueryInsertJobOperator(
        task_id="bigquery_dml_task",
        configuration={
            "query": {
                "query": read_and_prepare(sql+"Q001.sql", params),
                "useLegacySql": False,
            }
        },
        location=location,
    )


    t1 >> t2 >> t3 >> bq_task