from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from datetime import timedelta,datetime
from pandas_profiling import ProfileReport
import pandas as pd
import pytz

from functions import *
from etl import *

# Obtener la fecha actual
TZ = pytz.timezone('America/Bogota')
TODAY = datetime.now(TZ).strftime("%Y-%m-%d")


default_args = {
    'owner': 'michael',
    'retries': 2,
    'retry_delay': timedelta(minutes=0.5)
}

path_station = '/opt/airflow/dags/data/base/Electric and Alternative Fuel Charging Stations.csv'
path_veh_com = '/opt/airflow/dags/data/base/vehiculos_combustion_CO2_2023.csv'
path_con_son = '/opt/airflow/dags/data/base/Conta_Sonora.csv'
path_cal_air = '/opt/airflow/dags/data/base/Calidad_del_aire.csv'
path_taxis = '/opt/airflow/dags/data/base/taxis.parquet_2022_2023'



with DAG(
    'InitialLoading',
    description = 'Iniciando data pipeline',
    default_args = default_args,
    catchup = False,
    schedule_interval='@once',
    start_date = datetime(2023,1,1),
    tags = ['inicio']
    ) as dag:
    
    StartPipeline = EmptyOperator(
        task_id = 'StartPipeline',
        dag = dag
        )

    PythonLoad1 = PythonOperator(
        task_id="Load_Station",
        python_callable=LoadProducto,
        )

    PythonLoad2 = PythonOperator(
        task_id="Load_Veh_Com",
        python_callable=LoadSucursal,
        )

    PythonLoad3 = PythonOperator(
        task_id="Load_Con_Son",
        python_callable=LoadPrecios,
        )
    
    PythonLoad4 = PythonOperator(
        task_id="Load_Cal_air",
        python_callable=LoadPrecios,
        )
    
    FinishETL= EmptyOperator(
    task_id = 'FinishETL',
    dag = dag
    )

    SqlLoad = PythonOperator(
    task_id="SQLUploadAll",
    python_callable=UploadAll,
    )

    FinishSQLLoading = EmptyOperator(
        task_id = 'FinishSQLLoading',
        dag = dag
        )

    CheckWithQuery = PythonOperator(
        task_id="CheckWithQuery",
        python_callable=MakeQuery,
    )

    FinishPipeline = EmptyOperator(
    task_id = 'FinishPipeline',
    dag = dag
    )


StartPipeline >> [PythonLoad1, PythonLoad2, PythonLoad3,PythonLoad4] >> FinishETL

FinishETL >> SqlLoad >> FinishSQLLoading

FinishSQLLoading >> CheckWithQuery >> FinishPipeline