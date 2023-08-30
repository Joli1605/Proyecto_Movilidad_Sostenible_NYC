import pandas as pd
import glob

spacer = '*'*10
path_base = '/opt/airflow/dags/data/base/'
path_cleaned = '/opt/airflow/dags/data/cleaned/'
path_taxi = '/opt/airflow/dags/data/base/Taxis.parquet_2022_2023/'

from functions import *


def Load_Station():
    try:
        df = CleanStation(FileImporter('Electric and Alternative Fuel Charging Stations', 'csv', path = path_base))
        df.to_csv(f'{path_cleaned}Station_NY.csv', index=False)
        print('Station_NY Cleaned and Saved')
    except:
        print('Error cleaning Station_NY')

def Load_Veh_Com():
    try:
        df = Clean_Veh_Com(FileImporter('vehiculos_combustion_CO2_2023', 'csv', path = path_base))
        df.to_csv(f'{path_cleaned}vehiculos_combustion_CO2_2023.csv', index=False)
        print('vehiculos_combustion_CO2_2023 Cleaned and Saved')
    except:
        print('Error cleaning vehiculos_combustion_CO2_2023')

def Load_Con_Son():
    try:
        df = Clean_Con_Son(FileImporter('Conta_Sonora', 'csv', path = path_base))
        df.to_csv(f'{path_cleaned}Conta_Sonora.csv', index=False)
        print('Conta_Sonora Cleaned and Saved')
    except:
        print('Error cleaning Conta_Sonora')
        
def Load_Cal_Air():
    try:
        df = Clean_Cal_Air(FileImporter('Calidad_del_aire', 'csv', path = path_base))
        df.to_csv(f'{path_cleaned}Calidad_del_aire.csv', index=False)
        print('Calidad_del_aire Cleaned and Saved')
    except:
        print('Error cleaning Calidad_del_aire')

def Load_Taxis():
    try:
        df = FolderImporterTaxis(path_taxi)
        df.to_csv(f'{path_cleaned}taxis_tarifa.csv', index=False)
        print('taxis_tarifa Cleaned and Saved')
    except:
        print('Error cleaning taxis_tarifa')


def UploadAll():
    try:
        engine = ConnectSQL()
    except:
        print('Error connecting to SQL')

    base_files = glob.glob(f'{path_cleaned}*.csv')
    all_files_cleaned = base_files 

    for filename in all_files_cleaned:
        try:
            df = pd.read_csv(filename)
            df.to_sql(filename.split('/')[-1].split('.')[0].split('_')[0], con=engine, if_exists='replace', index=False)
            print('Successfully uploaded ', filename)
        except:
            print('Error uploading ', filename)
