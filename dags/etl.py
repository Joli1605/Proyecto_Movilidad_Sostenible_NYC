import pandas as pd
import glob
import pymysql
from sqlalchemy import create_engine

from datetime import timedelta,datetime
from pandas_profiling import ProfileReport
import pandas as pd
import pytz

# Obtener la fecha actual
TZ = pytz.timezone('America/Bogota')
TODAY = datetime.now(TZ).strftime("%Y-%m-%d")
TARGET = 'sql/crear_tablas.sql'


spacer = '*'*10
path_base = '/opt/airflow/dags/data/base/'
path_cleaned = '/opt/airflow/dags/data/cleaned/'
path_taxi_green = '/opt/airflow/dags/data/base/Taxis.parquet_2022_2023/taxi_green'
path_taxi_yellow = '/opt/airflow/dags/data/base/Taxis.parquet_2022_2023/taxi_yellow'
path_profiling = '/opt/airflow/dags/data/profiling/'
path_sql = '/opt/airflow/dags/sql/'

from functions import *


def Profi_Cal_Air():
    df = pd.read_csv(f'{path_base}Calidad_del_aire.csv')
    profile = ProfileReport(df, title='Reporte de calidad de los datos en calidad del aire') 
    profile.to_file('{}prof_cal_aire_{}.html'.format(path_profiling,TODAY))

def Load_Cal_Air():
    try:
        df = Clean_Cal_Air(FileImporter('Calidad_del_aire', 'csv', path = path_base))
        #df.to_csv(f'{path_cleaned}Calidad_del_aire.csv', index=False)
        df.to_csv('{}Calidad_del_aire_{}.csv'.format(path_cleaned, TODAY), index=False)
        print('Calidad_del_aire Cleaned and Saved')
    except:
        print('Error cleaning Calidad_del_aire')
    
    # iterar sobre las filas y crear los insert
    with open('{}Calidad_del_aire_{}.sql'.format(path_sql, TODAY), 'w') as f:
        for index, row in df.iterrows():
            values = f"('{row['Name']}','{row['Measure']}','{row['Measure_Info']}','{row['Geo_Type_Name']}',{row['Geo_Join_ID']},'{row['Geo_Place_Name']}','{row['Time_Period']}','{row['Start_Date']}',{row['Data_Value']})"
            insert = f"INSERT INTO Calidad_del_aire VALUES {values};\n"
            f.write(insert)

def Profi_Con_Son():
    df = pd.read_csv(f'{path_base}Conta_Sonora.csv')
    profile = ProfileReport(df, title='Reporte de calidad de los datos en contaminacion sonora') 
    profile.to_file('{}prof_con_son_{}.html'.format(path_profiling,TODAY))

def Load_Con_Son():
    try:
        df = Clean_Con_Son(FileImporter('Conta_Sonora', 'csv', path = path_base))
        #df.to_csv(f'{path_cleaned}Conta_sonora.csv', index=False)
        df.to_csv('{}Conta_sonora_{}.csv'.format(path_cleaned, TODAY), index=False)
        print('Conta_sonora Cleaned and Saved')
    except:
        print('Error cleaning Conta_sonora')
        
    # iterar sobre las filas y crear los insert
    with open('{}conta_sonora_{}.sql'.format(path_sql, TODAY), 'w') as f:
        for index, row in df.iterrows():
            values = f"('{row['fecha']}',{row['id_borough']},{row['engine_sounds']},{row['alert_signal_sounds']},{row['total_sounds']},'{row['borough_name']}')"
            insert = f"INSERT INTO conta_sonora VALUES {values};\n"
            f.write(insert)

def Profi_Clima():
    df = pd.read_csv(f'{path_base}NYCtiempo.csv')
    profile = ProfileReport(df, title='Reporte de calidad de los datos en NYCclima') 
    profile.to_file('{}prof_NYCclima_{}.html'.format(path_profiling,TODAY))

        
def Load_Clima():
    try:
        df = Clean_Clima(FileImporter('NYCtiempo', 'csv', path = path_base))
        #df.to_csv(f'{path_cleaned}NYCclima.csv', index=False)
        df.to_csv('{}NYCclima_{}.csv'.format(path_cleaned, TODAY), index=False)
        print('NYCclima Cleaned and Saved')
    except:
        print('Error cleaning NYCclima')
        
      # iterar sobre las filas y crear los insert
    with open('{}NYCclima_{}.sql'.format(path_sql, TODAY), 'w') as f:
        for index, row in df.iterrows():
            values = f"('{row['time']}','{row['hours']}',{row['temperature']})"
            insert = f"INSERT INTO NYCCLIMA VALUES {values};\n"
            f.write(insert)    

def Profi_Station():
    df = pd.read_csv(f'{path_base}Electric and Alternative Fuel Charging Stations.csv')
    profile = ProfileReport(df, title='Reporte de calidad de los datos en Station_NY') 
    profile.to_file('{}prof_Station_NY_{}.html'.format(path_profiling,TODAY))


def Load_Station():
    try:
        df = Clean_Station(FileImporter('Electric and Alternative Fuel Charging Stations', 'csv', path = path_base))
        #df.to_csv(f'{path_cleaned}Station_NY.csv', index=False)
        df.to_csv('{}Station_NY_{}.csv'.format(path_cleaned, TODAY), index=False)
        print('Station_NY Cleaned and Saved')
    except:
        print('Error cleaning Station_NY')
        
      # iterar sobre las filas y crear los insert
    with open('{}Station_NY_{}.sql'.format(path_sql, TODAY), 'w') as f:
        for index, row in df.iterrows():
            values = f"({row['ID']},'{row['Fuel_Type_Code']}','{row['City']}','{row['State']}',{row['ZIP']},'{row['Status_Code']}','{row['Groups_With_Access_Code']}','{row['Geocode_Status']}',{row['Latitude']},{row['Longitude']},'{row['Country']}','{row['Access_Code']}')"
            insert = f"INSERT INTO Station_NY VALUES {values};\n"
            f.write(insert)   

def Profi_Taxi_zones():
    df = pd.read_csv(f'{path_base}Taxi_Zones.csv')
    profile = ProfileReport(df, title='Reporte de calidad de los datos en Taxi_Zones') 
    profile.to_file('{}prof_Taxi_Zones_{}.html'.format(path_profiling,TODAY))


def Load_Taxi_zones():
    try:
        df = Clean_Taxi_Zones(FileImporter('Taxi_Zones', 'csv', path = path_base))
        #df.to_csv(f'{path_cleaned}Taxi_Zones.csv', index=False)
        df.to_csv('{}Taxi_Zones_{}.csv'.format(path_cleaned, TODAY), index=False)
        print('Taxi Zone Cleaned and Saved')
    except:
        print('Error cleaning Taxi Zone')
    
    with open('{}Taxi_Zones_{}.sql'.format(path_sql, TODAY), 'w') as f:
        for index, row in df.iterrows():
            values = f"({row['LocationID']},{row['Shape_Leng']},{row['Shape_Area']},'{row['borough']}',{row['latitude']},{row['longitude']})"
            insert = f"INSERT INTO Taxi_zones VALUES {values};\n"
            f.write(insert) 
        
def Load_TaxiG():
    try:
        df = FolderImporterTaxis_green(path_taxi_green)
        df.to_csv(f'{path_cleaned}taxiG.csv', index=False)
        print('taxiG Cleaned and Saved')
    except:
        print('Error cleaning taxiG')
    
    with open('{}taxiG_{}.sql'.format(path_sql, TODAY), 'w') as f:
        for index, row in df.iterrows():
            values = f"('{row['Fecha']}',{row['Pasajeros_por_dia']},{row['Viajes_por_dia']},{row['Tarifario_por_dia']},{row['Total_recaudado_por_dia']},{row['Pago_con_tarjeta']},{row['Pago_con_efectivo']},'{row['Tipo_de_Taxi']}')"
            insert = f"INSERT INTO taxiG VALUES {values};\n"
            f.write(insert) 
    

def Load_TaxiY():
    try:
        df = FolderImporterTaxis_yellow(path_taxi_yellow)
        df.to_csv(f'{path_cleaned}taxiY.csv', index=False)
        print('taxiY Cleaned and Saved')
    except:
        print('Error cleaning taxiY')
    
    with open('{}taxiY_{}.sql'.format(path_sql, TODAY), 'w') as f:
        for index, row in df.iterrows():
            values = f"('{row['Fecha']}',{row['Pasajeros_por_dia']},{row['Viajes_por_dia']},{row['Tarifario_por_dia']},{row['Total_recaudado_por_dia']},{row['Pago_con_tarjeta']},{row['Pago_con_efectivo']},'{row['Tipo_de_Taxi']}')"
            insert = f"INSERT INTO taxiY VALUES {values};\n"
            f.write(insert) 


def Load_Taxi_Tarifa():
    try:
        taxiG = pd.read_csv(f'{path_cleaned}taxiG.csv')
        taxiY = pd.read_csv(f'{path_cleaned}taxiY.csv')
        df = pd.concat([taxiG, taxiY], ignore_index=True)
        df.to_csv(f'{path_cleaned}taxis_tarifa.csv', index=False)
        print('taxis_tarifa Cleaned and Saved')
    except:
        print('Error cleaning taxis_tarifa')
        
    with open('{}taxis_tarifa_{}.sql'.format(path_sql, TODAY), 'w') as f:
        for index, row in df.iterrows():
            values = f"('{row['Fecha']}',{row['Pasajeros_por_dia']},{row['Viajes_por_dia']},{row['Tarifario_por_dia']},{row['Total_recaudado_por_dia']},{row['Pago_con_tarjeta']},{row['Pago_con_efectivo']},'{row['Tipo_de_Taxi']}')"
            insert = f"INSERT INTO taxis_tarifa VALUES {values};\n"
            f.write(insert) 

def Profi_Veh_Com():
    df = pd.read_csv(f'{path_base}vehiculos_combustion_CO2_2023.csv')
    profile = ProfileReport(df, title='Reporte de calidad de los datos en veh_com') 
    profile.to_file('{}prof_veh_com_{}.html'.format(path_profiling,TODAY))

def Load_Veh_Com():
    try:
        df = Clean_Veh_Com(FileImporter('vehiculos_combustion_CO2_2023', 'csv', path = path_base))
        #df.to_csv(f'{path_cleaned}veh_com.csv', index=False)
        df.to_csv('{}veh_com_{}.csv'.format(path_cleaned, TODAY), index=False)
        print('vehiculos_combustion Cleaned and Saved')
    except:
        print('Error cleaning vehiculos_combustion')
        
    with open('{}veh_com_{}.sql'.format(path_sql, TODAY), 'w') as f:
        for index, row in df.iterrows():
            values = f"({row['Model_Year']},'{row['Make']}','{row['Model_1']}','{row['Vehicle_Class']}',{row['Engine_Size']},{row['Cylinders']},'{row['Transmission']}',{row['Fuel_Type']},{row['Fuel_Consumption_City']},{row['CO2_Emissions']},{row['CO2_Rating']},{row['Smog_Rating']})"
            insert = f"INSERT INTO veh_com VALUES {values};\n"
            f.write(insert) 
