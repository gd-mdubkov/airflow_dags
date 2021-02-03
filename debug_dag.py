import logging
from datetime import timedelta
import time
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2021-01-01T14:20:00',
    'end_date': None,
    'email': ['mikhail_dubkov@apple.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30)
}


def test_and_sleep():
    logging.info("Start sleeping...")
    time.sleep(700)
    logging.info("Finished sleeping")
    raise RuntimeError("Fail.")


with DAG('debug_dag', default_args=default_args, schedule_interval='*/1 * * * *', catchup=False,
         is_paused_upon_creation=False) as dag:
    start_task = DummyOperator(task_id='start_task')

    finish_task = DummyOperator(task_id='finish_task')

    test_and_sleep_step = PythonOperator(
        task_id='test_and_sleep_task',
        python_callable=test_and_sleep,
    )
    start_task >> test_and_sleep_step >> finish_task
