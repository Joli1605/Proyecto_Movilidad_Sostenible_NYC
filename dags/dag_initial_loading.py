from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.task_group import TaskGroup

from datetime import timedelta,datetime
from pandas_profiling import ProfileReport
import pandas as pd
import pytz

from functions import *
from etl import *

# Obtener la fecha actual
TZ = pytz.timezone('America/Bogota')
TODAY = datetime.now(TZ).strftime("%Y-%m-%d")
TARGET = 'sql/'

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
    ##############################################################
    
    with TaskGroup("calidad_aire",tooltip="pipeline de datos") as calidad_aire :
        
        Profiling1 = PythonOperator(
            task_id = 'Profi_Cal_Air',
            python_callable = Profi_Cal_Air,
        )
        
        PythonLoad1 = PythonOperator(
            task_id="Load_Cal_Air",
            python_callable=Load_Cal_Air,
        )
        
        crear_tabla1 = PostgresOperator(
            task_id = 'Crear_Tabla_Cal_Air',
            postgres_conn_id = 'postgres_docker',
            sql = '''
            CREATE TABLE IF NOT EXISTS Calidad_del_aire (
            Name VARCHAR(255),
            Measure VARCHAR(255),
            Measure_Info VARCHAR(255),
            Geo_Type_Name VARCHAR(255),
            Geo_Join_ID INT,
            Geo_Place_Name VARCHAR(255),
            Time_Period VARCHAR(255),
            Start_Date DATE,
            Data_Value FLOAT
            );       
            '''
        )
        
        insert_tabla1 = PostgresOperator(
            task_id = 'Insert_Tabla_Cal_Air',
            postgres_conn_id = 'postgres_docker',
            sql = '{}Calidad_del_aire_{}.sql'.format(TARGET,TODAY)
        )
        
        FinishCalidadAire = EmptyOperator(
            task_id='FinishCalidadAire',
            dag=dag
        )
        
        Profiling1 >> PythonLoad1 >> crear_tabla1 >> insert_tabla1 >> FinishCalidadAire
           
        
    #####################################
    
    with TaskGroup("contaminacion_sonora",tooltip="pipeline de datos") as contaminacion_sonora :
    
        Profiling2 = PythonOperator(
            task_id = 'Profi_Con_Son',
            python_callable = Profi_Con_Son,
        )
        
        PythonLoad2 = PythonOperator(
            task_id="Load_Con_Son",
            python_callable=Load_Con_Son,
        )
        
        crear_tabla2 = PostgresOperator(
            task_id = 'Crear_Tabla_Con_Son',
            postgres_conn_id = 'postgres_docker',
            sql = '''
            CREATE TABLE IF NOT EXISTS conta_sonora (
            fecha DATE,
            id_borough INT,
            engine_sounds INT,
            alert_signal_sounds INT,
            total_sounds INT,
            borough_name VARCHAR(255)
            );
            '''
        )
        
        insert_tabla2 = PostgresOperator(
            task_id = 'Insert_Tabla_Con_Son',
            postgres_conn_id = 'postgres_docker',
            sql = '{}conta_sonora_{}.sql'.format(TARGET,TODAY)
        )
        
        FinishContaminacionSonora = EmptyOperator(
            task_id='FinishContaminacionSonora',
            dag=dag
        )
        
        Profiling2 >> PythonLoad2 >> crear_tabla2 >> insert_tabla2 >> FinishContaminacionSonora
    
    ################################################################################
    
    with TaskGroup("clima",tooltip="pipeline de datos") as clima :
    
        Profiling3 = PythonOperator(
            task_id = 'Profi_Clima',
            python_callable = Profi_Clima,
        )
        
        PythonLoad3 = PythonOperator(
            task_id="Load_Clima",
            python_callable=Load_Clima,
            )

        crear_tabla3 = PostgresOperator(
            task_id = 'Crear_Tabla_Clima',
            postgres_conn_id = 'postgres_docker',
            sql = '''
            CREATE TABLE IF NOT EXISTS NYCCLIMA (
            time DATE,
            hours TIME,
            temperature FLOAT
            );
            '''
        )
        
        insert_tabla3 = PostgresOperator(
            task_id = 'Insert_Tabla_Clima',
            postgres_conn_id = 'postgres_docker',
            sql = '{}NYCclima_{}.sql'.format(TARGET,TODAY)
        )
        
        FinishClima = EmptyOperator(
            task_id='FinishClima',
            dag=dag
        )
        
        Profiling3 >> PythonLoad3 >> crear_tabla3 >> insert_tabla3 >> FinishClima   

################################################################

    with TaskGroup("estaciones",tooltip="pipeline de datos") as estaciones:
        Profiling4 = PythonOperator(
            task_id = 'Profi_Station',
            python_callable = Profi_Station,
        )

        PythonLoad4 = PythonOperator(
            task_id="Load_Station",
            python_callable=Load_Station,
            )
        
        crear_tabla4 = PostgresOperator(
            task_id = 'Crear_Tabla_Station',
            postgres_conn_id = 'postgres_docker',
            sql = '''
            CREATE TABLE IF NOT EXISTS Station_NY (
            ID INT,
            Fuel_Type_Code VARCHAR(255),
            City VARCHAR(255),
            State VARCHAR(255),
            ZIP INT,
            Status_Code VARCHAR(255),
            Groups_With_Access_Code VARCHAR(255),
            Geocode_Status VARCHAR(255),
            Latitude FLOAT,
            Longitude FLOAT,
            Country VARCHAR(255),
            Access_Code VARCHAR(255)
            );
            '''
        )
        
        insert_tabla4 = PostgresOperator(
            task_id = 'Insert_Tabla_Station',
            postgres_conn_id = 'postgres_docker',
            sql = '{}Station_NY_{}.sql'.format(TARGET,TODAY)
        )
        
        FinishEstaciones = EmptyOperator(
            task_id='FinishEstaciones',
            dag=dag
        )
        
        Profiling4 >> PythonLoad4 >> crear_tabla4 >> insert_tabla4  >> FinishEstaciones
    
#####################################################################    
    
    with TaskGroup("taxi_zones",tooltip="pipeline de datos") as taxi_zones:
    
        Profiling5 = PythonOperator(
            task_id = 'Profi_Taxi_zones',
            python_callable = Profi_Taxi_zones,
        )
        
        PythonLoad5 = PythonOperator(
            task_id="Load_Taxi_zones",
            python_callable=Load_Taxi_zones,
        )
        
        crear_tabla5 = PostgresOperator(
            task_id = 'Crear_Taxi_zones',
            postgres_conn_id = 'postgres_docker',
            sql = '''
            CREATE TABLE IF NOT EXISTS Taxi_zones (
            LocationID INT,
            Shape_Leng FLOAT,
            Shape_Area FLOAT,
            borough VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT
            );
            '''
        )
        
        insert_tabla5 = PostgresOperator(
            task_id = 'Insert_Taxi_zones',
            postgres_conn_id = 'postgres_docker',
            sql = '{}Taxi_zones_{}.sql'.format(TARGET,TODAY)
        )   
        
        FinishTaxiZones = EmptyOperator(
            task_id='FinishTaxiZones',
            dag=dag
        )
        
        Profiling5 >> PythonLoad5 >> crear_tabla5 >> insert_tabla5 >> FinishTaxiZones
    
###################################################################################        
    
    with TaskGroup("taxiG",tooltip="pipeline de datos") as taxiG:
    
        PythonLoad6 = PythonOperator(
            task_id="Load_Taxi_G",
            python_callable=Load_TaxiG,
        )   
        
        crear_tabla6 = PostgresOperator(
            task_id = 'Crear_Taxi_G',
            postgres_conn_id = 'postgres_docker',
            sql = '''
            CREATE TABLE IF NOT EXISTS taxiG (
            Fecha Date,
            Pasajeros_por_dia INT,
            Viajes_por_dia INT,
            Tarifario_por_dia FLOAT,
            Total_recaudado_por_dia FLOAT,
            Pago_con_tarjeta INT,
            Pago_con_efectivo INT,
            Tipo_de_Taxi VARCHAR(255)
            );
            '''
        ) 
        
        insert_tabla6 = PostgresOperator(
            task_id = 'Insert_Taxi_G',
            postgres_conn_id = 'postgres_docker',
            sql = '{}taxiG_{}.sql'.format(TARGET,TODAY)
        ) 
        
        FinishTaxiG = EmptyOperator(
            task_id='FinishTaxiG',
            dag=dag
        )
        
        PythonLoad6 >> crear_tabla6 >> insert_tabla6 >> FinishTaxiG 
    
#############################################################################    

    with TaskGroup("taxiY",tooltip="pipeline de datos") as taxiY :
    
        PythonLoad7 = PythonOperator(
            task_id="Load_Taxi_Y",
            python_callable=Load_TaxiY,
            execution_timeout=timedelta(hours=0.5),
            )
        
        crear_tabla7 = PostgresOperator(
            task_id = 'Crear_Taxi_Y',
            postgres_conn_id = 'postgres_docker',
            sql = '''
            CREATE TABLE IF NOT EXISTS taxiY (
            Fecha Date,
            Pasajeros_por_dia INT,
            Viajes_por_dia INT,
            Tarifario_por_dia FLOAT,
            Total_recaudado_por_dia FLOAT,
            Pago_con_tarjeta INT,
            Pago_con_efectivo INT,
            Tipo_de_Taxi VARCHAR(255)
            );
            '''
        ) 
        
        insert_tabla7 = PostgresOperator(
            task_id = 'Insert_Taxi_Y',
            postgres_conn_id = 'postgres_docker',
            sql = '{}taxiY_{}.sql'.format(TARGET,TODAY)
        ) 
        
        FinishTaxiY = EmptyOperator(
            task_id='FinishTaxiY',
            dag=dag
        )
        
        PythonLoad7 >> crear_tabla7 >> insert_tabla7 >> FinishTaxiY  
    
    
#################################################################################
    
    with TaskGroup("taxi_tarifa",tooltip="pipeline de datos") as taxi_tarifa :
    
        PythonLoad8 = PythonOperator(
            task_id="Load_Taxi_Tarifa",
            python_callable=Load_Taxi_Tarifa,
        )
        
        crear_tabla8 = PostgresOperator(
            task_id = 'Crear_Taxi_Tarifa',
            postgres_conn_id = 'postgres_docker',
            sql = '''
            CREATE TABLE IF NOT EXISTS taxis_tarifa (
            Fecha Date,
            Pasajeros_por_dia INT,
            Viajes_por_dia INT,
            Tarifario_por_dia FLOAT,
            Total_recaudado_por_dia FLOAT,
            Pago_con_tarjeta INT,
            Pago_con_efectivo INT,
            Tipo_de_Taxi VARCHAR(255)
            );
            '''
        ) 
        
        insert_tabla8 = PostgresOperator(
            task_id = 'Insert_Taxi_Tarifa',
            postgres_conn_id = 'postgres_docker',
            sql = '{}taxis_tarifa_{}.sql'.format(TARGET,TODAY)
        ) 
        
        FinishTaxiTarifa = EmptyOperator(
            task_id='FinishTaxiTarifa',
            dag=dag
        )
        
        PythonLoad8 >> crear_tabla8 >> insert_tabla8 >> FinishTaxiTarifa 


########################################
    
    with TaskGroup("vehiculo_combustion",tooltip="pipeline de datos") as vehiculo_combustion :
    
        Profiling9 = PythonOperator(
            task_id = 'Profi_Veh_Com',
            python_callable = Profi_Veh_Com,
        )
        
        PythonLoad9 = PythonOperator(
            task_id="Load_Veh_Com",
            python_callable=Load_Veh_Com,
        )
        
        crear_tabla9 = PostgresOperator(
            task_id = 'Crear_Veh_Com',
            postgres_conn_id = 'postgres_docker',
            sql = '''
            CREATE TABLE IF NOT EXISTS veh_com (
            Model_Year INT,
            Make VARCHAR(255),
            Model_1 VARCHAR(255),
            Vehicle_Class VARCHAR(255),
            Engine_Size FLOAT,
            Cylinders FLOAT,
            Transmission VARCHAR(255),
            Fuel_Type FLOAT,
            Fuel_Consumption_City FLOAT,
            CO2_Emissions INT,
            CO2_Rating INT,
            Smog_Rating INT
            );
            '''
        ) 
        
        insert_tabla9 = PostgresOperator(
            task_id = 'Insert_Veh_Com',
            postgres_conn_id = 'postgres_docker',
            sql = '{}Veh_Com_{}.sql'.format(TARGET,TODAY)
        ) 
        
        FinishVehiculoCombustion = EmptyOperator(
            task_id='FinishVehiculoCombustion',
            dag=dag
        )
        
        Profiling9 >> PythonLoad9 >> crear_tabla9 >> insert_tabla9 >> FinishVehiculoCombustion
    
    
  #########################################
    FinishETL= EmptyOperator(
        task_id = 'FinishETL',
        dag = dag
    )


# Define las relaciones entre las tareas finales de los TaskGroups
StartPipeline >> calidad_aire >> contaminacion_sonora >> clima >> estaciones >> taxi_zones >> taxiG >> taxiY >> taxi_tarifa >> vehiculo_combustion >> FinishETL
