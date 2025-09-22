from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from random import randint

DAG_NAME = "TEST_FAIL"

def always_pass():
    print("Starting the DAG!")


def fail_purposefully():
    if randint(0,1)==0:
        print("failure (on purpose)")
        raise Exception("💥 This task failed on purpose!")
    else:
        print("success")


def handle_success():
    print("✅ maybe_fail_task succeeded! (This should NOT run in this example)")


def handle_failure():
    print("❌ maybe_fail_task failed! Handling failure...")

# Define the DAG
dag = DAG(
    dag_id=DAG_NAME,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
)

    start_task = PythonOperator(
        task_id="start_task",
        python_callable=always_pass,
    )

    maybe_fail = PythonOperator(
        task_id="maybe_fail_task",
        python_callable=fail_purposefully,
    )

    on_success = PythonOperator(
        task_id="on_success_task",
        python_callable=handle_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # default
    )

    on_failure = PythonOperator(
        task_id="on_failure_task",
        python_callable=handle_failure,
        trigger_rule=TriggerRule.ONE_FAILED,  # runs if maybe_fail fails
    )

    # DAG structure: start_task -> maybe_fail -> (on_success OR on_failure)
    start_task >> maybe_fail
    maybe_fail >> on_success
    maybe_fail >> on_failure
