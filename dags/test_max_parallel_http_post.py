from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.utils.dates import days_ago
from airflow.configuration import conf

# Fetch max parallel tasks from Airflow config
MAX_PARALLEL_TASKS = int(conf.get('core', 'max_active_tasks_per_dag', fallback=10))

# Define API details
HTTP_CONN_ID = "neomdev_test_endpoint"  # Define in Airflow Connections
API_ENDPOINT = ""
HEADERS = {"Content-Type": "application/json"}

# Define DAG
with DAG(
    dag_id="test_max_parallel_http_post",
    default_args={"retries": 2},
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_tasks=MAX_PARALLEL_TASKS,  # Control parallel execution
    max_active_tis_per_dag=MAX_PARALLEL_TASKS,  # Limit concurrent task instances
) as dag:

    # Create multiple HTTP POST tasks in parallel
    tasks = [
        HttpOperator(
            task_id=f"post_request_{i}",
            http_conn_id=HTTP_CONN_ID,
            endpoint=API_ENDPOINT,
            method="POST",
            headers=HEADERS,
            data=f'{{"task_id": "task_{i}", "message": "Hello from Task {i}"}}',
            log_response=True,
        ) for i in range(MAX_PARALLEL_TASKS)
    ]

    # Run all tasks in parallel
    for task in tasks:
        task

if __name__ == "__main__":
    dag.test()
