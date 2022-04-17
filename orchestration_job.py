import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from etl_job.load_job import load_data
from etl_job.transform_job import transform_data
from common_objects.constants import START_DATE, END_DATE
from etl_job.report_job import fill_report_table

default_args = {
    'owner': 'OzgurTekingoz',
    'start_date': datetime.datetime(2022, 4, 16),
    'retries': 1,
    'execution_timeout': datetime.timedelta(hours=1),
    'provide_context': True
}

with DAG(
        dag_id='Ozgur_Tekingoz_Firefly_Case_ETL',
        catchup=False,
        schedule_interval='* 14 * * *',
        default_args=default_args,
        is_paused_upon_creation=True,
        max_active_runs=1) as dag:
    load_data_task = PythonOperator(
        task_id='load_data_to_bq_task',
        dag=dag,
        python_callable=load_data,
        op_kwargs={'start_date': START_DATE, 'end_date': END_DATE},
        provide_context=True
    )

    transform_data_task = PythonOperator(
        task_id='transform_data_task',
        dag=dag,
        python_callable=transform_data,
        op_kwargs={'start_date': START_DATE, 'end_date': END_DATE},
        provide_context=True)

    report_finalize_task = PythonOperator(
        task_id='finalize_report_data',
        dag=dag,
        python_callable=fill_report_table,
        op_kwargs={'start_date': START_DATE, 'end_date': END_DATE},
        provide_context=True)

    load_data_task >> transform_data_task >> report_finalize_task
