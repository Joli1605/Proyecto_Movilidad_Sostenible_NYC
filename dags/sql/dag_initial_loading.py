from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.task_group import TaskGroup
#from airflow.utils.dates import days_ago

from datetime import timedelta,datetime
from pandas_profiling import ProfileReport
import pandas as pd
import pytz

# Obtener la fecha actual
TZ = pytz.timezone('America/Bogota')
TODAY = datetime.now(TZ).strftime("%Y-%m-%d")


url_station = 'https://raw.githubusercontent.com/marcebalzarelli/Proyecto_Movilidad_Sostenible_NYC/main/Datasets/Dataset_empresa/Electric%20and%20Alternative%20Fuel%20Charging%20Stations.csv' 
path_station = '/opt/airflow/dags/data/Electric and Alternative Fuel Charging Stations.csv'
output_dq = '/opt/airflow/dags/data/data_quality_report_{}.html'.format(TODAY)
output_sql = '/opt/airflow/dags/sql/station_ny_{}.sql'.format(TODAY)
output = '/opt/airflow/dags/data/station_ny_{}.csv'.format(TODAY)
#output = '/opt/airflow/dags/data/station_ny.csv'

def _profile():
    df = pd.read_csv(path_station)
    profile = ProfileReport(df, title = 'data quality report')
    profile.to_file(output_dq)
    
    
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
    
    #iterar sobre las filas y crear los insert
    
    with open(output_sql, 'w') as f:
        for index, row in station_ny.iterrows():
            values = f"({row['ID']}, '{row['Fuel Type Code']}', '{row['Station Name']}', '{row['City']}', '{row['State']}', '{row['EV Level1 EVSE Num']}', '{row['EV Level2 EVSE Num']}', '{row['EV DC Fast Count']}', '{row['EV Network']}', '{row['Geocode Status']}', '{row['Latitude']}', '{row['Longitude']}', '{row['NG Vehicle Class']}', '{row['EV Connector Types']}', '{row['Groups With Access Code (French)']}', '{row['Access Detail Code']}', '{row['CNG Dispenser Num']}', '{row['CNG Vehicle Class']}', '{row['LNG Vehicle Class']}')" 
            insert = f"INSERT INTO raw_station VALUES {values};\n"
            f.write(insert)


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
    start_date = datetime(2023,1,1),
    schedule_interval = '@once',
    tags = ['inicio']
    
) as dag:
      
    '''descargar = BashOperator(
       task_id = 'descargar_csv_station',
       bash_command = 'curl -o {{params.path}} {{params.url}}', 
       params = {
            'path' : path_station,
            'url' : url_station
        }
     )'''
     
    profiling = PythonOperator(
        task_id = 'profiling',
        python_callable = _profile
    )   
        
        
    curated = PythonOperator (
        task_id = 'curated',
        python_callable = _curated
 
    )    
    
    create_raw = MySqlOperator(
        task_id = 'create_raw',
        mysql_conn_id='mysql_docker',
        sql = """
            CREATE TABLE IF NOT EXISTS raw_titanic {
                ID INT,
                Fuel Type Code VARCHAR(10),
                Station Name VARCHAR(90),
                City VARCHAR(40),
                State VARCHAR(40),
                EV Level1 EVSE Num FLOAT,
                EV Level2 EVSE Num FLOAT,
                EV DC Fast Count FLOAT,
                EV Network VARCHAR(50),
                Geocode Status VARCHAR(50),
                Latitude FLOAT,
                Longitude FLOAT,
                NG Vehicle Class VARCHAR(50),
                EV Connector Types VARCHAR(50),
                Groups With Access Code (French) VARCHAR(50),
                Access Detail Code VARCHAR(50),
                CNG Dispenser Num FLOAT,
                CNG Vehicle Class VARCHAR(50),
                LNG Vehicle Class VARCHAR(50)  
            }
        """        
    )
        
        