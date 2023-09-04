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
path_taxi_green = '/opt/airflow/dags/data/base/Taxis.parquet_2022_2023/taxi_green'
path_taxi_yellow = '/opt/airflow/dags/data/base/Taxis.parquet_2022_2023/taxi_yellow'
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

def etl_taxi_green(file_path):
    
    # STEP 1: Abrimos el archivo .parquet
    green_febrero = pd.read_parquet(file_path)

    # STEP 2: Extraemos la fecha de recolección y resumir los pasajeros por día
    green_febrero['pickup_date'] = green_febrero['lpep_pickup_datetime'].dt.date
    df_resumen = green_febrero.groupby('pickup_date').agg({'passenger_count': 'sum'}).reset_index()

    # STEP 3: Calcular la cantidad de viajes por día
    cantidad_viajes_por_dia = green_febrero['pickup_date'].value_counts().reset_index()
    cantidad_viajes_por_dia.columns = ['pickup_date', 'cantidad_viajes']
    df_resumen = df_resumen.merge(cantidad_viajes_por_dia, on='pickup_date', how='left')

    # STEP 4: Calcular el monto total de tarifas por día
    df_resumen_dias = green_febrero.groupby('pickup_date')['fare_amount'].sum().reset_index()
    df_resumen = df_resumen.merge(df_resumen_dias, on='pickup_date', how='left')

    # STEP 5: Calcular el monto total por día
    df_monto_total = green_febrero.groupby('pickup_date')['total_amount'].sum().reset_index()
    df_resumen = df_resumen.merge(df_monto_total, on='pickup_date', how='left')

    # STEP 6: Transformar la columna 'payment_type' en columnas separadas para cada tipo de pago (one-hot encoding)
    green_febrero['payment_type'] = green_febrero['payment_type'].fillna(6.0)
    green_febrero['payment_type'] = green_febrero['payment_type'].astype('int')
    df_payment_types = pd.get_dummies(green_febrero['payment_type'], prefix='payment_type')

    # Agregar la columna 'pickup_date' al nuevo DataFrame
    df_payment_types['pickup_date'] = green_febrero['pickup_date']

    # Combinar las filas con la misma fecha usando groupby y sumar los valores
    df_payment_types = df_payment_types.groupby('pickup_date').sum()
    df_resumen = df_resumen.merge(df_payment_types, on='pickup_date', how='left')

    return df_resumen

def merge_taxi_green(file_paths):
    dfs = []
    
    for file_path in file_paths:
        # usamos la funcion etl_process
        df_resumen_mes = etl_taxi_green(file_path)
        
        # agregamos los dataframes resumidos
        dfs.append(df_resumen_mes)
    
    # concatenamos todos los dataFrames en uno solo
    df_final = pd.concat(dfs, ignore_index=True)
    
    # Ordanamos todos los datos en funcion a la columna 'pickup_date'
    df_final = df_final.sort_values(by='pickup_date')
    
    # R
    df_final = df_final.reset_index(drop=True)
    return df_final

def FolderImporterTaxis_green(path:str = path_taxi_green, spacer:str = ',', spacer_txt:str = '|'):

    #Get all files in the folder
    try:

        all_parquet = glob.glob(path + "/*.parquet")
        

        if len(all_parquet) == 0:
            raise FileNotFoundError('No files found in the folder')

    except:
        print('Error with path or files GLOB ERROR')

    #Get all PARQUET in the folder
    if len(all_parquet) > 0:
       # Llamar a la función para obtener el DataFrame final
        taxiG = merge_taxi_green(all_parquet)
        
        print('All PARQUET files imported and cleaned successfully')
    else:
        print('No PARQUET files found')

    # Borraremos los datos atipicos.
    taxiG = taxiG[10:]
    # Seleccionamos las columnas que usaremos en este proyecto.
    taxiG = taxiG[['pickup_date','passenger_count','cantidad_viajes','fare_amount','total_amount','payment_type_1','payment_type_2']]
    # Ahora renombraremos las columnas.
    columnas = {'pickup_date':'Fecha','passenger_count':'Pasajeros por dia','cantidad_viajes':'Viajes por dia','fare_amount':'Tarifario por dia','total_amount':'Total recaudado por dia','payment_type_1':'Pago con tarjeta','payment_type_2':'Pago con efectivo'}
    taxiG.rename(columns=columnas, inplace=True)
    taxiG['Tipo de Taxi'] = 'green'
    taxiG['Pasajeros por dia']= taxiG['Pasajeros por dia'].astype('int')
    taxiG['Pago con efectivo']=taxiG['Pago con efectivo'].astype('int')
    taxiG['Pago con tarjeta'] = taxiG['Pago con tarjeta'].astype('int')
    # Crear un diccionario de mapeo de nombres de columnas
    mapeo_nombres = {
        'Fecha': 'Fecha',
        'Pasajeros por dia': 'Pasajeros_por_dia',
        'Viajes por dia': 'Viajes_por_dia',
        'Tarifario por dia': 'Tarifario_por_dia',
        'Total recaudado por dia': 'Total_recaudado_por_dia',
        'Pago con tarjeta': 'Pago_con_tarjeta',
        'Pago con efectivo': 'Pago_con_efectivo',
        'Tipo de Taxi': 'Tipo_de_Taxi'
    }

    # Renombrar las columnas en el DataFrame
    taxiG.rename(columns=mapeo_nombres, inplace=True)
    

    return taxiG

def etl_taxi_yellow(file_path):
    # STEP 1: Abrimos el archivo .parquet
    green_febrero = pd.read_parquet(file_path)

    # STEP 2: Extraemos la fecha de recolección y resumir los pasajeros por día
    green_febrero['pickup_date'] = green_febrero['tpep_pickup_datetime'].dt.date
    df_resumen = green_febrero.groupby('pickup_date').agg({'passenger_count': 'sum'}).reset_index()

    # STEP 3: Calcular la cantidad de viajes por día
    cantidad_viajes_por_dia = green_febrero['pickup_date'].value_counts().reset_index()
    cantidad_viajes_por_dia.columns = ['pickup_date', 'cantidad_viajes']
    df_resumen = df_resumen.merge(cantidad_viajes_por_dia, on='pickup_date', how='left')

    # STEP 4: Calcular el monto total de tarifas por día
    df_resumen_dias = green_febrero.groupby('pickup_date')['fare_amount'].sum().reset_index()
    df_resumen = df_resumen.merge(df_resumen_dias, on='pickup_date', how='left')

    # STEP 5: Calcular el monto total por día
    df_monto_total = green_febrero.groupby('pickup_date')['total_amount'].sum().reset_index()
    df_resumen = df_resumen.merge(df_monto_total, on='pickup_date', how='left')

    # STEP 6: Transformar la columna 'payment_type' en columnas separadas para cada tipo de pago (one-hot encoding)
    green_febrero['payment_type'] = green_febrero['payment_type'].fillna(6.0)
    green_febrero['payment_type'] = green_febrero['payment_type'].astype('int')
    df_payment_types = pd.get_dummies(green_febrero['payment_type'], prefix='payment_type')

    # Agregar la columna 'pickup_date' al nuevo DataFrame
    df_payment_types['pickup_date'] = green_febrero['pickup_date']

    # Combinar las filas con la misma fecha usando groupby y sumar los valores
    df_payment_types = df_payment_types.groupby('pickup_date').sum()
    df_resumen = df_resumen.merge(df_payment_types, on='pickup_date', how='left')

    return df_resumen

def merge_taxi_yellow(file_paths):
    dfs = []
    
    for file_path in file_paths:
        # usamos la funcion etl_process2
        df_resumen_mes = etl_taxi_yellow(file_path)
        
        # agregamos los dataframes resumidos
        dfs.append(df_resumen_mes)
    
    # concatenamos todos los dataFrames en uno solo
    df_final = pd.concat(dfs, ignore_index=True)
    
    # Ordanamos todos los datos en funcion a la columna 'pickup_date'
    df_final = df_final.sort_values(by='pickup_date')
    
    # R
    df_final = df_final.reset_index(drop=True)
    return df_final

def FolderImporterTaxis_yellow(path:str = path_taxi_yellow, spacer:str = ',', spacer_txt:str = '|'):

    #Get all files in the folder
    try:

        all_parquet = glob.glob(path + "/*.parquet")
        

        if len(all_parquet) == 0:
            raise FileNotFoundError('No files found in the folder')

    except:
        print('Error with path or files GLOB ERROR')

    #Get all PARQUET in the folder
    if len(all_parquet) > 0:
       # Llamar a la función para obtener el DataFrame final
        taxiY = merge_taxi_yellow(all_parquet)
        
        print('All PARQUET files imported and cleaned successfully')
    else:
        print('No PARQUET files found')
        
    # Borramos valores atipicos.
    taxiY = taxiY[53:]
    # Seleccionamos las columnas que usaremos en este proyecto.
    taxiY = taxiY[['pickup_date','passenger_count','cantidad_viajes','fare_amount','total_amount','payment_type_0','payment_type_1']]
    # Ahora renombraremos las columnas.
    columnas = {'pickup_date':'Fecha','passenger_count':'Pasajeros por dia','cantidad_viajes':'Viajes por dia','fare_amount':'Tarifario por dia','total_amount':'Total recaudado por dia','payment_type_0':'Pago con tarjeta','payment_type_1':'Pago con efectivo'}
    taxiY=taxiY.rename(columns=columnas)
    # Agregamos la columna de tipo de taxi
    taxiY['Tipo de Taxi'] = 'yellow'
    # Convertimos las columnas a un formato mas trabajable.
    taxiY['Pasajeros por dia'] = taxiY['Pasajeros por dia'].astype('int')
    taxiY['Pago con efectivo'] = taxiY['Pago con efectivo'].astype('int')
    taxiY['Pago con tarjeta'] = taxiY['Pago con tarjeta'].astype('int')
    # Crear un diccionario de mapeo de nombres de columnas
    mapeo_nombres = {
        'Fecha': 'Fecha',
        'Pasajeros por dia': 'Pasajeros_por_dia',
        'Viajes por dia': 'Viajes_por_dia',
        'Tarifario por dia': 'Tarifario_por_dia',
        'Total recaudado por dia': 'Total_recaudado_por_dia',
        'Pago con tarjeta': 'Pago_con_tarjeta',
        'Pago con efectivo': 'Pago_con_efectivo',
        'Tipo de Taxi': 'Tipo_de_Taxi'
    }

    # Renombrar las columnas en el DataFrame
    taxiY.rename(columns=mapeo_nombres, inplace=True)

    return taxiY


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

