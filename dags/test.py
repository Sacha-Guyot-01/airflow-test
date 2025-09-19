from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Harmless Python functions for each task
def task_one():
    print("✅ Task 1 completed")

def task_two():
    raise Exception("Test")

def task_three():
    print("✅ Task 3 completed")

# Define the DAG
with DAG(
    dag_id="three_task_test",
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

    # Set task dependencies: t1 -> t2 -> t3
    t1 >> t2 >> t3
