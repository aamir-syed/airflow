from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.http.operators.http import HttpOperator
import json
from airflow.utils.session import provide_session
from airflow.models.xcom import LazySelectSequence, XCom
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.models.xcom_arg import XComArg
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # ✅ Works with Minio

POSTGRES_CONN_ID = 'rega.geodb'
CHUNK_SIZE = 1

S3_BUCKET_NAME = "airflow-data"
S3_CONN_ID = "minio_conn"
S3_XCOM_PREFIX_TEMPLATE = "xcom/{dag_id}/{run_id}/"


# Default retry policy
default_args = {
    'retries': 5,  
    'retry_delay': timedelta(seconds=5),
}


class ExtractDataOperator(BaseOperator):
    """Extracts data from Postgres and stores it in XCom in chunks."""
    def __init__(self, task_index, **kwargs):
        super().__init__(**kwargs)
        self.task_index = task_index

    def execute(self, context):
        ti = context['ti']

        data = {
    "features": [
        {
            "geometry": {
                "coordinates": [
                    46.789751129,
                    24.806160537
                ],
                "type": "Point"
            },
            "id": "3221125",
            "properties": {
                "additional_number": "6811",
                "building_number": "3409",
                "contract_end_date": "2030-08-31T00:00:00",
                "contract_number": "10406283134",
                "contract_start_date": "2024-07-01T00:00:00",
                "contractid": "3E61D835-9C01-4937-9A30-0D009D9B7FEA",
                "lkcitynamear": "الرياض",
                "lkcontractstatear": "نشط",
                "lkcontracttypear": "سكني",
                "lkdistrictnamear": "اليرموك",
                "lkinvoicespaymentmethodnamear": "سداد",
                "lkpropertytypear": "عمارة",
                "lkpropertyusagear": "متعدد",
                "lkregionnamear": "الرياض",
                "lkunitfinishingar": "مشطب",
                "lkunittypear": "شقة",
                "lkunitusagear": "سكن عائلات",
                "perioddays": 2252,
                "postal_code": "13251",
                "propertyid": "9BB10D1D-E194-469B-B3E3-FC89ACEEA492",
                "registerdate": "2024-05-02T16:14:59",
                "total_floors": 3,
                "total_rooms": 2,
                "unitarea": 100,
                "units_per_floor": 10
            },
            "type": "Feature"
        }
],
    "type": "FeatureCollection"
}

        valid_features = [f for f in data.get('features', []) if f.get('geometry') and f.get('properties')]
        data_chunks = [{"type": "FeatureCollection", "features": valid_features[i:i + CHUNK_SIZE]} 
                       for i in range(0, len(valid_features), CHUNK_SIZE)]

        print(f"✅ Valid Features : {len(valid_features)}")

        for j, chunk in enumerate(data_chunks):
            ti.xcom_push(key=f'extract_data_{self.task_index}_{j}', value=chunk)
            print(f"✅ Pushed XCom for extract_data_{self.task_index}_{j}")

        if not data_chunks:
            ti.xcom_push(key=f'extract_data_{self.task_index}_0', value=[])
            print(f"✅ Pushed empty XCom for extract_data_{self.task_index}_0")



with DAG(
    dag_id='test_xcom_delete',
    schedule=None,
    start_date=datetime(2025, 1, 27),
    catchup=False,
    default_args=default_args,
    tags=["Test XCOM delete"],
    params={
        "TOTAL_TASKS": Param(
            default=100,
            type="integer",
            description="Total Number of Extracts task. Default is 100",
        ),
    },
) as dag:
    
    def get_records_task(**kwargs):

        # Retrieve TOTAL_TASKS from DAG params
        total_tasks = kwargs["params"]["TOTAL_TASKS"]
        kwargs['ti'].xcom_push(key="total_tasks", value=total_tasks)

        print(f"✅ TOTAL_TASKS: {total_tasks}")

    get_records_task_op = PythonOperator(
        task_id="get_records_per_task",
        python_callable=get_records_task,
        provide_context=True,
        dag=dag
    )

    def generate_task_indices(**kwargs):
        ti = kwargs['ti']
        total_tasks = ti.xcom_pull(task_ids="get_records_per_task", key="total_tasks")
        if total_tasks is None:
            raise ValueError("❌ total_tasks not found in XCom!")

        return list(range(total_tasks))  # ✅ Return a dynamically generated list

    generate_task_indices_op = PythonOperator(
        task_id="generate_task_indices",
        python_callable=generate_task_indices,
        provide_context=True,
        dag=dag
    )


    def delete_s3_xcom_files(**kwargs):
        """Deletes all XCom-related files in Minio S3 for the current DAG run."""
        dag_id = kwargs["dag"].dag_id
        run_id = kwargs["run_id"]
    
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)  # ✅ Works with Minio if S3_CONN_ID is configured
        s3_prefix = S3_XCOM_PREFIX_TEMPLATE.format(dag_id=dag_id, run_id=run_id)
    
        objects_to_delete = s3_hook.list_keys(bucket_name=S3_BUCKET_NAME, prefix=s3_prefix)
    
        if objects_to_delete:
            s3_hook.delete_objects(bucket=S3_BUCKET_NAME, keys=objects_to_delete)
            print(f"🧹 Deleted {len(objects_to_delete)} XCom objects from Minio: {s3_prefix}")
        else:
            print(f"✅ No XCom files found under: {s3_prefix}")
    
    clear_s3_xcom_task = PythonOperator(
        task_id="clear_s3_xcom_task",
        python_callable=delete_s3_xcom_files,
        provide_context=True,
        dag=dag
    ) 

    extract_tasks = ExtractDataOperator.partial(
        task_id='extract_data',
        retries=5,
        retry_delay=timedelta(seconds=5)
    ).expand(task_index=XComArg(generate_task_indices_op)) 
        
    get_records_task_op >> generate_task_indices_op >> extract_tasks >> clear_s3_xcom_task
