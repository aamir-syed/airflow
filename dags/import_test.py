from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import json

# Default arguments for the DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    'parallel_http_load_test',
    default_args=default_args,
    schedule_interval=None,  # This DAG is meant to be triggered manually
    catchup=False,
) as dag:

    @task
    def generate_data(num_items):
        """Generates a list of dictionaries with random data."""
        data = []
        for _ in range(num_items):
            data.append({
                'id': random.randint(1, 100000),
                'value': random.random() * 100,
                'timestamp': str(datetime.now()),
            })
        return data

    # Generate data for the load test
    test_data = generate_data(num_items=10000)  # Adjust the number of items as needed

    # Split the data into chunks for parallel processing
    chunk_size = 1000  # Adjust chunk size as needed
    data_chunks = [
        test_data[x:x+chunk_size] for x in range(0, len(test_data), chunk_size)
    ]

    # Create a list to hold the tasks
    load_tasks = []

    # Create a task for each chunk of data
    for i, chunk in enumerate(data_chunks):

        # Define the task to send the data via HTTP POST
        load_task = HttpOperator(
            task_id=f'load_chunk_{i}',
            http_conn_id='your_http_conn_id',  # Replace with your HTTP connection ID
            endpoint='/your/api/endpoint',  # Replace with your API endpoint
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({'data': chunk}),
        )

        load_tasks.append(load_task)

    # Set dependencies to run tasks in parallel
    # In this example, all load_tasks run independently
    # You can adjust this to create different parallel patterns if needed