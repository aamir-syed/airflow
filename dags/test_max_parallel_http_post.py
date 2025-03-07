from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.utils.dates import days_ago
from airflow.configuration import conf

# Fetch the max parallel tasks from Airflow config
MAX_PARALLEL_TASKS = int(conf.get('core', 'max_active_tasks_per_dag', fallback=16))

# Define the DAG
with DAG(
    dag_id="test_max_parallel_http_post",
    default_args={"retries": 2},
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_tasks=MAX_PARALLEL_TASKS,  # Max parallel tasks allowed
) as dag:
    
    # Create multiple parallel HTTP tasks
    tasks = [
        HttpOperator(
            task_id=f"http_post_task_{i}",
            http_conn_id="neomdev_test_endpoint",  # Set this in Airflow Connections
            endpoint="",
            method="POST",
            headers={"Content-Type": "application/json"},
            data='{"param": "value"}',
            log_response=True,
        ) for i in range(MAX_PARALLEL_TASKS)
    ]

    # Set parallel execution
    for task in tasks:
        task

if __name__ == "__main__":
    dag.test()
