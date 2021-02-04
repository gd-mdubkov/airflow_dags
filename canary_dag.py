import logging
from datetime import timedelta

import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2021-01-07',
    'email': ['mdubkov@griddynamics.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30)
}


def grid_health_check():
    r = requests.get('http://google.com')

    if r.status_code != 200:
        raise Exception(f"Google website request failed with code: {r.status_code}, body: {r.content}")

    logging.info("Google website responds with '%s'", r.content)



with DAG('canary_dag', default_args=default_args, schedule_interval='*/1 * * * *', catchup=True,
         is_paused_upon_creation=True) as dag:
    dummy_task = DummyOperator(task_id='start_task')

    check_data_processor_health = PythonOperator(
        task_id='grid_health_check',
        python_callable=grid_health_check,
    )


dummy_task >> check_data_processor_health
