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
        task_id="Load_Cal_Air",
        python_callable=Load_Cal_Air,
        )
    
    PythonLoad2 = PythonOperator(
        task_id="Load_Con_Son",
        python_callable=Load_Con_Son,
        )
    
    PythonLoad3 = PythonOperator(
        task_id="Load_Clima",
        python_callable=Load_Clima,
        )

    PythonLoad4 = PythonOperator(
        task_id="Load_Station",
        python_callable=Load_Station,
        )
    
    PythonLoad5 = PythonOperator(
        task_id="Load_Taxi_zones",
        python_callable=Load_Taxi_zones,
        )
        
    PythonLoad6 = PythonOperator(
        task_id="Load_Taxi_G",
        python_callable=Load_TaxiG,
        )   
    
    PythonLoad7 = PythonOperator(
        task_id="Load_Taxi_Tarifa",
        python_callable=Load_Taxi_Tarifa,
        )
    
    PythonLoad8 = PythonOperator(
        task_id="Load_Taxi_Y",
        python_callable=Load_TaxiY,
        )

    PythonLoad9 = PythonOperator(
        task_id="Load_Veh_Com",
        python_callable=Load_Veh_Com,
        )
  
    FinishETL= EmptyOperator(
    task_id = 'FinishETL',
    dag = dag
    )

    #SqlLoad = PythonOperator(
    #task_id="SQLUploadAll",
    #python_callable=UploadAll,
    #mysql_conn_id = 'mysql_docker'
    #)

    #FinishSQLLoading = EmptyOperator(
    #    task_id = 'FinishSQLLoading',
    #    dag = dag
    #    )

    #CheckWithQuery = PythonOperator(
    #    task_id="CheckWithQuery",
        #python_callable=MakeQuery,
    #)

    #FinishPipeline = EmptyOperator(
    #task_id = 'FinishPipeline',
    #dag = dag
    #)


#StartPipeline >> [PythonLoad1, PythonLoad2, PythonLoad3,PythonLoad4,PythonLoad5,PythonLoad6,PythonLoad7,PythonLoad8,PythonLoad9] >> FinishETL

#FinishETL >> SqlLoad >> FinishSQLLoading

#FinishSQLLoading >> CheckWithQuery >> FinishPipeline