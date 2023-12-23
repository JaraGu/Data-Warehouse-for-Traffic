from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import dag, task
from datetime import datetime
from docker.types import Mount


@dag(
    # owner="traffic",
    start_date=datetime.now(),
    schedule_interval='@hourly',
    catchup=False)
def docker_dag():
    start_dag = DummyOperator(
        task_id='start_dag'
    )

    end_dag = DummyOperator(
        task_id='end_dag'
    )

    # Configuration
    image_name = 'apache/airflow:2.8.0'
    docker_url = 'unix://var/run/docker.sock'
    source_path = '/sources/dags'
    target_path = '/opt/airflow/dags'

    el_dag = DockerOperator(
        task_id='extract_and_load',
        image=image_name,
        container_name="eltaskdag",
        docker_url=docker_url,
        network_mode="bridge",  #
        mounts=[
            Mount(
                target=target_path,
                source=source_path,
                type="bind"
            )
        ],
    )

    start_dag >> el_dag >> end_dag


dag = docker_dag()
