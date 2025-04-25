from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['b86882@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 14),
}

dag = DAG(
    'local_orders_backfilling_dag',
    default_args=default_args,
    description='Run local PySpark job with a date argument using SparkSubmitOperator',
    schedule_interval=None,
    catchup=False,
    tags=['local'],
    params={
        'execution_date': Param(default='NA', type='string', description='Execution date in yyyymmdd format'),
    }
)

def get_execution_date(ds_nodash, **kwargs):
    execution_date = kwargs['params'].get('execution_date', 'NA')
    if execution_date == 'NA':
        execution_date = ds_nodash
    return execution_date

get_execution_date_task = PythonOperator(
    task_id='get_execution_date',
    python_callable=get_execution_date,
    op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    do_xcom_push=True,
    dag=dag,
)

run_spark_job = SparkSubmitOperator(
    task_id='run_pyspark_job_local',
    application='/home/bittupraneeth/airflow/jobs/orders_data_process.py',
    name='orders_data_process',
    conn_id='spark_local',  # this must match the Connection ID in Admin
    conf={"spark.master": "local[*]"},
    application_args=[
        '--date', '{{ ti.xcom_pull(task_ids="get_execution_date") }}'
    ],
    # conn_id='spark_local',  # Use your actual Spark connection ID
    # conf={"spark.master": "local[*]"},
    do_xcom_push=True,
    dag=dag,
)

get_execution_date_task >> run_spark_job
