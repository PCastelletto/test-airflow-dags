import logging
import docker

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

def do_test_docker():
    logging.debug('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA')
    client = docker.from_env()
    print(len(client.images()))
    for image in client.images():
        logging.info(str(image))

def launch_docker_container(**context):
    # just a mock for now
    logging.info('Asdf')
    image_name = context['image_name']
    client = docker.from_env()
    container = client.create_container(image=image_name)
    container_id = container.get('Id')
    logging.info(f"Running container with id {container_id}")
    client.start(container=container_id)
    logs = client.logs(container_id, follow=True, stderr=True, stdout=True, stream=True, tail='all')
    try:
        while True:
            l = next(logs)
            logging.info(f"Task log: {l}")
    except StopIteration:
        pass

    inspect = client.inspect_container(container)
    logging.info(inspect)
    if inspect['State']['ExitCode'] != 0:
                raise Exception("Container has not finished with exit code 0")

    logging.info(f"Task ends!")

dag = DAG('hello_worldN', description='Hello world example', schedule_interval='0 12 * * *', start_date=datetime(2017, 3, 20), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_taskN', retries = 3, dag=dag)

hello_operator = PythonOperator(task_id='hello_taskN', python_callable=print_hello, dag=dag)

hellow_operator = PythonOperator(task_id='sumN', python_callable=sumsum,dag=dag)

bash_operator = BashOperator(task_id='bash_taskN',bash_command='echo "HELLOOOOOOOO!!!!!"',dag=dag)

bash_operator2 = BashOperator(task_id='bash_taskN2',bash_command='echo "HELLOOOOOOOO!!!!!"',dag=dag)

dockertest = PythonOperator(task_id="test_docker", python_callable=do_test_docker, dag=dag)

docker_launch_test = PythonOperator(task_id="test_docker_launch", python_callable=launch_docker_container, op_kwargs={
    'image_name':'task2',
    'my_id': 'super_id'
    } ,dag=dag)

dummy_operator >> hello_operator >> hellow_operator >> bash_operator >> bash_operator2 >> dockertest >> docker_launch_test


