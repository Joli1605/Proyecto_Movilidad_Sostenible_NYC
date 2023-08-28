from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
#import datetime
from datetime import timedelta,datetime
import pandas as pd
import pytz

# Obtener la fecha actual
#TODAY = datetime.datetime.now().strftime("%Y-%m-%d")
#start_date = datetime.datetime(2023, 1, 1)


url_station = 'https://raw.githubusercontent.com/marcebalzarelli/Proyecto_Movilidad_Sostenible_NYC/main/Datasets/Dataset_empresa/Electric%20and%20Alternative%20Fuel%20Charging%20Stations.csv' 
path_station = '/opt/airflow/dags/data/Electric and Alternative Fuel Charging Stations.csv'
#output = '/opt/airflow/dags/data/station_ny_{}.csv'.format(TODAY)
output = '/opt/airflow/dags/data/station_ny.csv'

def _curated():
    
    station = pd.read_csv(path_station)

    #Filtro por ubicación
    station_ny = station[station['State'] == 'NY']
    # Lista de nombres de columnas a eliminar
    columnas_a_eliminar = ['Street Address','Intersection Directions','ZIP','Plus4','Station Phone','Status Code','Groups With Access Code',
    'Access Days Time','Cards Accepted','Date Last Confirmed','Updated At','Owner Type Code','Federal Agency ID',
    'Open Date','Country','Access Code','Facility Type','CNG On-Site Renewable Source','CNG Total Compression Capacity','CNG Storage Capacity','EV Pricing',
    'LPG Nozzle Types','CNG Fill Type Code','CNG PSI','EV On-Site Renewable Source','Restricted Access','Expected Date','BD Blends','NG Fill Type Code','NG PSI',
    'EV Other Info','EV Network Web','Hydrogen Status Link','LPG Primary', 'E85 Blender Pump', 'Intersection Directions (French)','Access Days Time (French)','BD Blends (French)',
    'Hydrogen Is Retail','Federal Agency Code','LNG On-Site Renewable Source','E85 Other Ethanol Blends','EV Pricing (French)','Hydrogen Pressures','Hydrogen Standards','Federal Agency Name'
    ]

    # Elimino las columnas especificadas
    station_ny = station_ny.drop(columns=columnas_a_eliminar)
    # Reorganizo las columnas
    column_order = ['ID'] + [col for col in station_ny.columns if col != 'ID']
    station_ny=station_ny[column_order]
    #guardo
    station_ny.to_csv(output,index=False)   

default_args = {
    'owner': 'michael',
    #'depends_on_past': False,
    #'email_on_failure': False,
    #'email_on_retry': False,
    'retries': 2,
    #'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=0.5)
}

with DAG(
    'InitialLoading',
    description = 'Iniciando data pipeline',
    default_args = default_args,
    catchup = False,
    start_date = datetime(2023,1,1),
    schedule_interval = '@once',
    tags = ['inicio']
    
) as dag:
  
    '''
     descargar = BashOperator(
       task_id = 'descargar_csv_station',
       bash_command = 'curl -o {{params.path_station}} {{params.url_station}}', 
       params = {
            'path_station' : path_station,
            'url_station' : url_station
        }
     )'''
        
    curated = PythonOperator (
        task_id = 'curated',
        python_callable = _curated
 
    )    
    
        
        