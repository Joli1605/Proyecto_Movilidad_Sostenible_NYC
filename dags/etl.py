import pandas as pd
import glob
import pymysql
from sqlalchemy import create_engine

spacer = '*'*10
path_base = '/opt/airflow/dags/data/base/'
path_cleaned = '/opt/airflow/dags/data/cleaned/'
path_taxi_green = '/opt/airflow/dags/data/base/Taxis.parquet_2022_2023/taxi_green'
path_taxi_yellow = '/opt/airflow/dags/data/base/Taxis.parquet_2022_2023/taxi_yellow'

from functions import *


def Load_Cal_Air():
    try:
        df = Clean_Cal_Air(FileImporter('Calidad_del_aire', 'csv', path = path_base))
        df.to_csv(f'{path_cleaned}Calidad_del_aire.csv', index=False)
        print('Calidad_del_aire Cleaned and Saved')
    except:
        print('Error cleaning Calidad_del_aire')

def Load_Con_Son():
    try:
        df = Clean_Con_Son(FileImporter('Conta_Sonora', 'csv', path = path_base))
        df.to_csv(f'{path_cleaned}Conta_sonora.csv', index=False)
        print('Conta_sonora Cleaned and Saved')
    except:
        print('Error cleaning Conta_sonora')
        
def Load_Clima():
    try:
        df = Clean_Clima(FileImporter('NYCtiempo', 'csv', path = path_base))
        df.to_csv(f'{path_cleaned}NYCclima.csv', index=False)
        print('NYCclima Cleaned and Saved')
    except:
        print('Error cleaning NYCclima')

def Load_Station():
    try:
        df = Clean_Station(FileImporter('Electric and Alternative Fuel Charging Stations', 'csv', path = path_base))
        df.to_csv(f'{path_cleaned}Station_NY.csv', index=False)
        print('Station_NY Cleaned and Saved')
    except:
        print('Error cleaning Station_NY')

def Load_Taxi_zones():
    try:
        df = Clean_Taxi_Zones(FileImporter('Taxi_Zones', 'csv', path = path_base))
        df.to_csv(f'{path_cleaned}Taxi_Zones.csv', index=False)
        print('Taxi Zone Cleaned and Saved')
    except:
        print('Error cleaning Taxi Zone')

def Load_TaxiG():
    try:
        df = FolderImporterTaxis_green(path_taxi_green)
        df.to_csv(f'{path_cleaned}taxiG.csv', index=False)
        print('taxiG Cleaned and Saved')
    except:
        print('Error cleaning taxiG')

def Load_TaxiY():
    try:
        df = FolderImporterTaxis_yellow(path_taxi_yellow)
        df.to_csv(f'{path_cleaned}taxiY.csv', index=False)
        print('taxiY Cleaned and Saved')
    except:
        print('Error cleaning taxiY')


def Load_Taxi_Tarifa():
    try:
        taxiG = pd.read_csv(f'{path_cleaned}taxiG.csv')
        taxiY = pd.read_csv(f'{path_cleaned}taxiY.csv')
        df = pd.concat([taxiG, taxiY], ignore_index=True)
        df.to_csv(f'{path_cleaned}taxis_tarifa.csv', index=False)
        print('taxis_tarifa Cleaned and Saved')
    except:
        print('Error cleaning taxis_tarifa')

def Load_Veh_Com():
    try:
        df = Clean_Veh_Com(FileImporter('vehiculos_combustion_CO2_2023', 'csv', path = path_base))
        df.to_csv(f'{path_cleaned}veh_com.csv', index=False)
        print('vehiculos_combustion Cleaned and Saved')
    except:
        print('Error cleaning vehiculos_combustion')


def UploadAll():
    try:
        engine = ConnectSQL()
        #print("conexion exitosa")
    except Exception as e:
        print('Error connecting to SQL:', str(e))

    """""
    all_files_cleaned = glob.glob(f'{path_cleaned}*.csv')

    for filename in all_files_cleaned:
        try:
            df = pd.read_csv(filename)
            table_name = filename.split('/')[-1].split('.')[0].split('_')[0]
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print('Successfully uploaded', filename)
        except pd.errors.EmptyDataError as e:
            print('Empty data error for', filename, ':', str(e))
        except pd.errors.ParserError as e:
            print('Parser error for', filename, ':', str(e))
        except Exception as e:
            print('Error uploading', filename, ':', str(e))
"""
