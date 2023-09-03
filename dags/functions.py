import pandas as pd
import numpy as np
import glob
from sqlalchemy import create_engine
import os
from airflow.models.taskinstance import TaskInstance as ti
#from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile
import datetime as dt 

spacer = '*'*10
path_taxi = '/opt/airflow/dags/data/base/Taxis.parquet_2022_2023/'
path_other = '/opt/airflow/dags/datasets/base/'

#Import a single file, 
# name = filename
# tipo = extension file,
# path = path to file, 
# spacer = separator for CSV/TXT
# encoding = encoding for CSV/TXT

# Importar archivos
def FileImporter (name: str, tipo: str, spacer:str = ',', path:str = path_other, encoding:str = 'utf-8', sheet:int = 0):

    #Raise and error if type of file is not declared
    if tipo == '':
        raise ValueError ('You need to put some extension ir order to import the file')

    #Set the path to the file and extension
    file = path + name + '.' + tipo
    
    #DEBUG
    #print(file)
    
    try:
        #CSV with encoding error
        if tipo == 'csv':
            try:
                df = pd.read_csv(file, sep=spacer, encoding=encoding, low_memory=False)
                return df
            except UnicodeDecodeError as e:
                print('Try a different encoding method for the file', e)
        #XLS/XLSX
        elif tipo == 'xls' or tipo == 'xlsx':
            df = pd.read_excel(file, sheet_name = sheet)
            return df
        
        #JSON
        elif tipo == 'json':
            df = pd.read_json(file)
            return df

        #TXT
        elif tipo == 'txt':
            df = pd.read_csv(file, sep=spacer, encoding='utf-8')
            return df

        #PARQUET
        elif tipo == 'parquet':
            df = pd.read_parquet(file)
            return df
            
    except FileNotFoundError as f:
        print('Error reading file' + str(f))

    finally:
        print('Importing successfully done for ', file)

# ETL calidad del aire
def Clean_Cal_Air(df):
    # cambiar nombres de columnas
    nombres_nuevos = {
        'Name': 'Name',
        'Measure': 'Measure',
        'Measure Info': 'Measure_Info',
        'Geo Type Name': 'Geo_Type_Name',
        'Geo Join ID': 'Geo_Join_ID',
        'Geo Place Name': 'Geo_Place_Name',
        'Time Period': 'Time_Period',
        'Start_Date': 'Start_Date',
        'Data Value': 'Data_Value'
    }

    df.rename(columns=nombres_nuevos, inplace=True)
    return df

# ETL cotaminacion sonora
def Clean_Con_Son(df):
    #dataset extraido se encuentra limpio, no se realiza ninguna transformacion.
    pass
    return df

# ETL clima
def Clean_Clima(df):
    
    # Convertir la columna 'time' a tipo 'datetime'
    df['time'] = pd.to_datetime(df['time'])
    
    # creo columnas fecha y hora, elimino time
    df['fecha'] = df['time'].dt.date
    df['hora'] = df['time'].dt.time
    df.drop(columns=['time'], inplace=True)
    
    df = df[['fecha', 'hora'] + [col for col in df.columns if col not in ['fecha', 'hora']]]

    # Cambiar el nombre de la columna 'fecha' a 'time'
    df.rename(columns={'fecha': 'time'}, inplace=True)
    # Cambiar el nombre de la columna 'hora' a 'hours'
    df.rename(columns={'hora': 'hours'}, inplace=True)
    # Cambiar el nombre de la columna 'temperature_2m (°C)' a 'temperature'
    df.rename(columns={'temperature_2m (°C)': 'temperature'}, inplace=True)
    
    # Convertir la columna 'time' a tipo 'datetime'
    df['time'] = pd.to_datetime(df['time'])
    
    # Filtrar los registros para excluir el año 2020
    df = df[df['time'].dt.year != 2020]

    # Eliminar las columnas no deseadas
    columns_to_drop = ["precipitation (mm)", "rain (mm)", "is_day ()"]
    df = df.drop(columns=columns_to_drop)
    
    return df

# ETL estaciones
def Clean_Station(df):
           
    #Filtro por ubicación
    df = df[df['State'] == 'NY']
    # Eliminar filas con valores nulos del DataFrame original
    df.dropna(axis=1, inplace=True)
    # Reorganizo las columnas
    column_order = ['ID'] + [col for col in df.columns if col != 'ID']
    df=df[column_order]
    
    # Crear un diccionario de mapeo de nombres de columnas
    mapeo_nombres = {
        'ID': 'ID',
        'Fuel Type Code': 'Fuel_Type_Code',
        'Station Name': 'Station_Name',
        'City': 'City',
        'State': 'State',
        'Status Code': 'Status_Code',
        'Groups With Access Code': 'Groups_With_Access_Code',
        'Latitude': 'Latitude',
        'Longitude': 'Longitude',
        'Updated At': 'Updated_At',
        'Country': 'Country',
        'Groups With Access Code (French)': 'Groups_With_Access_Code_French',
        'Access Code': 'Access_Code'
    }

    # Renombrar las columnas en el DataFrame
    df.rename(columns=mapeo_nombres, inplace=True)
    
    return df

# ETL Taxis zona
def Clean_Taxi_Zones(df):
    #transformaciones
    columns_to_drop = ['Unnamed: 0', 'OBJECTID']
    df = df.drop(columns=columns_to_drop)

    column_order = ['LocationID'] + [col for col in df.columns if col != 'LocationID']
    df=df[column_order]

    df = df.rename(columns={'y': 'longitude', 'x': 'latitude'})
    return df


# ETL vehiculos de combustion
def Clean_Veh_Com(df):
    # Crear un diccionario de mapeo de nombres de columnas
    mapeo_nombres = {
        'Model(Year)': 'Model_Year',
        'Make': 'Make',
        'Model.1': 'Model_1',
        'Vehicle Class': 'Vehicle_Class',
        'Engine Size(L)': 'Engine_Size',
        'Cylinders': 'Cylinders',
        'Transmission': 'Transmission',
        'Fuel(Type)': 'Fuel_Type',
        'Fuel Consumption(City (L/100 km)': 'Fuel_Consumption_City',
        'CO2 Emissions(g/km)': 'CO2_Emissions',
        'CO2(Rating)': 'CO2_Rating',
        'Smog(Rating)': 'Smog_Rating'
    }

    # Renombrar las columnas en el DataFrame
    df.rename(columns=mapeo_nombres, inplace=True)
    return df

def FolderImporterTaxis(path:str = path_taxi, spacer:str = ',', spacer_txt:str = '|'):

    #Get all files in the folder
    try:
        all_csv = glob.glob(path + "/*.csv")
        all_xls = glob.glob(path + "/*.xls") +  glob.glob(path + "/*.xlsx")
        all_json = glob.glob(path + "/*.json")
        all_txt = glob.glob(path + "/*.txt")
        all_parquet = glob.glob(path + "/*.parquet")

        all_files = all_csv + all_xls + all_json + all_txt + all_parquet

        if len(all_files) == 0:
            raise FileNotFoundError('No files found in the folder')

    except:
        print('Error with path or files GLOB ERROR')

    #Make lists for each type of file
    li_csv = []
    li_xls = []
    li_json = []
    li_txt = []
    li_parquet = []
    precio_final = []


    #Get all CSV in the folder
    if len(all_csv) > 0:
        for filename in all_csv:
            try:
                df = pd.read_csv(filename, sep=spacer, encoding='utf-8', low_memory=False)
                li_csv.append(Clean_Taxis(df))
            except:
                df = pd.read_csv(filename, sep=spacer, encoding='utf-16', low_memory=False)
                li_csv.append(Clean_Taxis(df))
                print('File imported with utf-16 encoding')
            finally:
                print('Importing successfully done for ', filename)
        
        print('All CSV files imported and cleaned successfully')
    else:
        print('No CSV files found')

    
    #Get all XLS/XLSX in the folder
    if len(all_xls) > 0:
        try:
            for filename in all_xls:
                df = pd.read_excel(filename, parse_dates=False, sheet_name=None, dtype={'precio': float, 'sucursal_id': object, 'producto_id': object})
                if type(df) == dict:
                    for key in df:
                        li_xls.append(Clean_Taxis(df[key]))
                else:
                    li_xls.append(Clean_Taxis(df))
        except:
            print('Error importing XLS/XLSX files')
        finally:
            print('Importing successfully done for ', filename)
        
        print('All XLS/XLSX files imported and cleaned successfully')
    else:
        print('No XLS/XLSX files found')


    #Get all JSON in the folder
    if len(all_json) > 0:
        for filename in all_json:
            df = pd.read_json(filename)
            li_json.append(Clean_Taxis(df))
        
        print('All JSON files imported and cleaned successfully')
    else:
        print('No JSON files found')


    #Get all TXT in the folder
    if len(all_txt) > 0:
        for filename in all_txt:
            try:
                df = pd.read_csv(filename, sep=spacer_txt, encoding='utf-8')
                li_txt.append(Clean_Taxis(df))
            except:
                print('Error with encoding, not UTF-8 probably', filename)
                df = pd.read_csv(filename, sep=spacer_txt, encoding='utf-16')
                li_txt.append(Clean_Taxis(df))
            finally:
                print('Importing successfully done for ', filename)
        
        print('All TXT files imported and cleaned successfully')
    else:
        print('No TXT files found')

    #Get all PARQUET in the folder
    if len(all_parquet) > 0:
        for filename in all_parquet:
            df = pd.read_parquet(filename)
            li_parquet.append(Clean_Taxis(df))
        
        print('All PARQUET files imported and cleaned successfully')
    else:
        print('No PARQUET files found')

    #Concatenate all files
    precio_final = pd.concat(li_csv + li_xls + li_json + li_txt + li_parquet, axis=0, ignore_index=True)
    return precio_final

# Export files to SQL
# Create sqlalchemy engine
# BEWARE OF IP ADDRESS IT CAN CHANGE WITH WIFI ROUTER RESTART
def ConnectSQL():
    try:
        engine = create_engine("mysql+pymysql://{user}:{pw}@{address}/{db}"
                    .format(user="root",
                            address = '127.0.0.1:3306',
                            pw="8195",
                            db="proyecto_ny"))
        return engine
    except:
        print('Error connecting to SQL')

# Get a list of files in the folder to compare
def GetFiles():
    #Get all files in the folder
    try:
        all_csv = glob.glob(path_taxi + "/*.csv")
        all_xls = glob.glob(path_taxi + "/*.xls") +  glob.glob(path_taxi + "/*.xlsx")
        all_json = glob.glob(path_taxi + "/*.json")
        all_txt = glob.glob(path_taxi + "/*.txt")
        all_parquet = glob.glob(path_taxi + "/*.parquet")

        all_files = all_csv + all_xls + all_json + all_txt + all_parquet
        
        if len(all_files) == 0:
            raise FileNotFoundError('No files found in the folder')

    except:
        print('Error with path or files GLOB ERROR')

    return all_files
