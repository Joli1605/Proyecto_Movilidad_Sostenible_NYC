from airflow import DAG
from airflow.operador.bash_operator import BashOperator
from airflow.operators.python_operator import PyhtonOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.task_group import TaskGroup

from datetime import timedelta, datetime
import pandas as pd
import pytz


default_args = {
    'owner': 'michael',
    #'depends_on_past': False,
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 2,
    #'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=0.5)
}

path_base = '/opt/airflow/dags/datasets/base/'
path_precio = '/opt/airflow/dags/datasets/prices/'


with DAG(
    'InitialLoading',
    descripcion = 'iniciando data pipeline'
    default_args=default_args,
    catchup = False,
    start_date = datetime(2023,1,1)
    schedule_interval='@once',
    tags = ['inicio']
    
    ) as dag:
    