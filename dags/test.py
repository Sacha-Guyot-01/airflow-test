from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime

DAG_NAME = "three_task_test"

# Harmless Python functions for each task
def task_one():
    print("✅ Task 1 completed")

def task_two():
    print("✅ Task 2 completed")

def task_three():
    print("✅ Task 3 completed")

# Define the DAG
with DAG(
    dag_id=DAG_NAME,
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Run manually
    catchup=False,
    tags=["test"],
) as dag:

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

    bq_task = BigQueryExecuteQueryOperator(
        task_id="bigquery_dml_task",
        sql=f"""MERGE INTO `mailovepoc`.TEST.T1 tgt
             USING (SELECT '{DAG_NAME}' AS dag_name, CURRENT_TIMESTAMP AS last_run) AS src
             ON tgt.dag_name = src.dag_name
             WHEN MATCHED THEN UPDATE
             SET tgt.last_run = src.last_run
             WHEN NOT MATCHED BY TARGET THEN INSERT
             (dag_name, last_run)
             VALUES
             (src.dag_name, src.last_run);""",
        use_legacy_sql=False,
        location="US",
    )

    t1 >> t2 >> t3 >> bq_task