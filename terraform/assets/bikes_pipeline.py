import datetime
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.secret_manager import GoogleCloudSecretManagerHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateTableOperator, BigQueryDeleteTableOperator, BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator

DAG_PROJECT_ID = os.getenv("DAG_PROJECT_ID")
DAG_BUCKET_NAME = os.getenv("DAG_BUCKET_NAME")
DAG_DATASET_NAME = os.getenv("DAG_DATASET_NAME")
DAG_PROJECT_NUMBER = os.getenv("DAG_PROJECT_NUMBER")

def get_prev_month(ds):
    date = datetime.datetime.strptime(ds, "%Y-%m-%d")
    first_day = date.replace(day=1)
    prev_month = first_day - datetime.timedelta(days=1)

    if date.day < 15:
        first_day = prev_month.replace(day=1)
        prev_month = first_day - datetime.timedelta(days=1)

    return prev_month.strftime("%Y-%m")

default_args = {
    'start_date': datetime.datetime(2020, 5, 15),
    'retries': 1,
    "depends_on_past":False,
    "retry_delay": datetime.timedelta(seconds=5),
    "max_retry_delay": datetime.timedelta(seconds=5),
    "retry_exponential_backoff": False
}

dag = DAG(
    'Divvy_Bikes_Pipeline_Orch',
    default_args=default_args,
    description='Divvy Bikes Pipeline Dag',
    max_active_runs=1,
    schedule='0 0 15 * *',
    catchup=True,
    user_defined_macros={
        'get_prev_month': get_prev_month
    }
)

download_data = BashOperator(
    task_id='download_data',
    bash_command="""
    mkdir -p /tmp/data && \
    curl -L -o /tmp/data/bike-data-{{ get_prev_month(ds)[:4] }}-{{ get_prev_month(ds)[5:7] }}.zip https://divvy-tripdata.s3.amazonaws.com/{{ get_prev_month(ds)[:4] }}{{ get_prev_month(ds)[5:7] }}-divvy-tripdata.zip
    """,
    dag=dag,
    depends_on_past=False,
    do_xcom_push=False
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='unzip -o /tmp/data/bike-data-{{ get_prev_month(ds)[:4] }}-{{ get_prev_month(ds)[5:7] }}.zip -d /tmp/data',
    dag=dag,
    depends_on_past=False,
    do_xcom_push=False
)

upload_to_gcs = BashOperator(
    task_id='upload_to_gcs',
    bash_command='gcloud storage cp /tmp/data/{{ get_prev_month(ds)[:4] }}{{ get_prev_month(ds)[5:7] }}*.csv gs://$DAG_BUCKET_NAME/data/{{ get_prev_month(ds)[:4] }}{{ get_prev_month(ds)[5:7] }}-divvy-tripdata.csv',
    dag=dag
)

delete_external_table = BigQueryDeleteTableOperator(
    task_id='delete_external_table',
    deletion_dataset_table=f"{DAG_PROJECT_ID}.{DAG_DATASET_NAME}.bikes_ext",
    ignore_if_missing=True,
    dag=dag
)

create_external_table = GCSToBigQueryOperator(
    task_id='create_external_table',
    bucket=DAG_BUCKET_NAME ,
    source_objects=['data/{{ get_prev_month(ds)[:4] }}{{ get_prev_month(ds)[5:7] }}-divvy-tripdata.csv'],
    destination_project_dataset_table=f"{DAG_PROJECT_ID}.{DAG_DATASET_NAME}.bikes_ext",
    external_table=True,
    source_format='CSV',
    write_disposition='WRITE_TRUNCATE', 
    create_disposition='CREATE_IF_NEEDED',
    allow_jagged_rows=True,
    autodetect=True,
    allow_quoted_newlines=True,
    quote_character='"',
    schema_fields=[
        {'name': 'ride_id', 'type': 'STRING'},
        {'name': 'rideable_type', 'type': 'STRING'},
        {'name': 'started_at', 'type': 'TIMESTAMP'},
        {'name': 'ended_at', 'type': 'TIMESTAMP'},
        {'name': 'start_station_name', 'type': 'STRING'},
        {'name': 'start_station_id', 'type': 'STRING'},
        {'name': 'end_station_name', 'type': 'STRING'},
        {'name': 'end_station_id', 'type': 'STRING'},
        {'name': 'start_lat', 'type': 'FLOAT'},
        {'name': 'start_lng', 'type': 'FLOAT'},
        {'name': 'end_lat', 'type': 'FLOAT'},
        {'name': 'end_lng', 'type': 'FLOAT'},
        {'name': 'member_casual', 'type': 'STRING'}
        ],
    dag=dag
)

create_managed_table = BigQueryCreateEmptyTableOperator(
    task_id='create_managed_table',
    project_id=DAG_PROJECT_ID ,
    dataset_id=DAG_DATASET_NAME,
    table_id='bikes_managed',
    schema_fields=[
        {'name': 'ride_id', 'type': 'STRING'},
        {'name': 'rideable_type', 'type': 'STRING'},
        {'name': 'started_at', 'type': 'TIMESTAMP'},
        {'name': 'ended_at', 'type': 'TIMESTAMP'},
        {'name': 'start_station_name', 'type': 'STRING'},
        {'name': 'start_station_id', 'type': 'STRING'},
        {'name': 'end_station_name', 'type': 'STRING'},
        {'name': 'end_station_id', 'type': 'STRING'},
        {'name': 'start_lat', 'type': 'FLOAT'},
        {'name': 'start_lng', 'type': 'FLOAT'},
        {'name': 'end_lat', 'type': 'FLOAT'},
        {'name': 'end_lng', 'type': 'FLOAT'},
        {'name': 'member_casual', 'type': 'STRING'}
        ],
    time_partitioning={
        "type": "DAY",
        "field": "started_at",
    },
    cluster_fields=['start_station_id', 'end_station_id'],
    dag=dag
)

merge_data = BigQueryInsertJobOperator(
    task_id='merge_data',
    configuration={
        "query": {
            "query": f"""
            MERGE INTO `{DAG_PROJECT_ID}.{DAG_DATASET_NAME}.bikes_managed` M
                USING (
                    SELECT DISTINCT * FROM `{DAG_PROJECT_ID}.{DAG_DATASET_NAME}.bikes_ext`
                ) S
                ON M.ride_id = S.ride_id
                WHEN NOT MATCHED THEN
                    INSERT (ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual)
                    VALUES (S.ride_id, S.rideable_type, S.started_at, S.ended_at, S.start_station_name, S.start_station_id, S.end_station_name, S.end_station_id, S.start_lat, S.start_lng, S.end_lat, S.end_lng, S.member_casual);
            """,
            "useLegacySql": False,
        }
    },
    dag=dag
)

def should_run(**context):
    execution_date = int(context['execution_date'].month)

    last_execution_date = int(datetime.datetime.now().month)
    if datetime.datetime.now().day < 15:
        last_execution_date -= 1

    if context['execution_date'].year == datetime.datetime.now().year:
        if (last_execution_date - execution_date) == 1 :
            return 'trigger_ingestion_dag'
        elif (last_execution_date - execution_date) == 0 :
            return 'write_sa_key_to_file'
    
    return 'skip_all'  

branching_condition = BranchPythonOperator(
  task_id='branching_condition',
  python_callable=should_run
)

trigger_ingestion_dag = TriggerDagRunOperator(
    task_id="trigger_ingestion_dag",
    trigger_dag_id="Divvy_Bikes_Pipeline_Orch",
    execution_date='{{ next_execution_date }}',
    dag=dag,
)

skip_all = DummyOperator(
    task_id='skip_all',
    dag=dag
)

def write_sa_key_to_file():
    hook = GoogleCloudSecretManagerHook()
    secret_str = hook.access_secret(secret_id="pipeline_service_account", project_id=DAG_PROJECT_NUMBER, secret_version=1)
    with open("/tmp/pipeline_sa.json", "w") as f:
        print(secret_str.payload.data.decode("UTF-8"))
        f.write(secret_str.payload.data.decode("UTF-8"))

write_secret = PythonOperator(
    task_id="write_sa_key_to_file",
    python_callable=write_sa_key_to_file,
    dag=dag
)

setup_dbt_profile = BashOperator(
    task_id='setup_dbt_profile',
    bash_command=f"""
    mkdir -p ~/.dbt && \
    echo '
    default:
      target: dev
      outputs:
        dev:
          type: bigquery
          method: service-account
          project: {DAG_PROJECT_ID}
          dataset: {DAG_DATASET_NAME}
          threads: 8
          keyfile: /tmp/pipeline_sa.json
    ' > ~/.dbt/profiles.yml
    """,
    dag=dag
)


clone_repo = BashOperator(
    task_id='clone_dbt_repo',
    bash_command='rm -rf /tmp/divvy_bikes_project && git clone https://github.com/AmmarrOsama/Divvy-Bikes-Pipeline.git /tmp/divvy_bikes_project',
    dag=dag
)

run_dbt = BashOperator(
    task_id="run_dbt_project",
    bash_command="cd /tmp/divvy_bikes_project/dbt_project && dbt run --profiles-dir ~/.dbt",
    dag=dag
)

download_data >> unzip_data >> upload_to_gcs >> delete_external_table >> create_external_table >> \
create_managed_table >> merge_data >> branching_condition >> [trigger_ingestion_dag,skip_all,write_secret]

write_secret >> setup_dbt_profile >> clone_repo >> run_dbt
