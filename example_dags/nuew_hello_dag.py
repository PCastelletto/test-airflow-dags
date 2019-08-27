import logging

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

def print_hello():
    print('HELOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO')
    return 'Hello Wolrd!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'

def sumsum():
    return 10 + 10000000

dag = DAG('hello_world', description='Hello world example', schedule_interval='0 12 * * *', start_date=datetime(2017, 3, 20), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_taskN', retries = 3, dag=dag)

hello_operator = PythonOperator(task_id='hello_taskN', python_callable=print_hello, dag=dag)

hellow_operator = PythonOperator(task_id='sumN', python_callable=sumsum,dag=dag)

bash_operator = BashOperator(task_id='bash_taskN',bash_command='echo "HELLOOOOOOOO!!!!!"',dag=dag)

bash_operator2 = BashOperator(task_id='bash_taskN2',bash_command='echo "HELLOOOOOOOO!!!!!"',dag=dag)

dockertest = PythonOperator(task_id="test_docker", python_callable=do_test_docker, dag=dag)

dummy_operator >> hello_operator >> hellow_operator >> bash_operator >> bash_operator2 >> dockertest


