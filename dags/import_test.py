from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from datetime import datetime
from airflow.utils.dates import days_ago

with DAG(
    dag_id='http_parallel_tasks_test',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['http', 'parallel'],
) as dag:

    for i in range(50):  # Creating 50 parallel tasks
        http_task = HttpOperator(
            task_id=f'http_post_task_{i}',
            http_conn_id='http_default', #ensure you have a connection setup in airflow.
            endpoint='your_test_endpoint', #replace with your test endpoint.
            method='POST',
            data={'task_id': i},
            headers={'Content-Type': 'application/json'},
            log_response=True,
            response_check=lambda response: True, #always pass, just for testing.
        )
