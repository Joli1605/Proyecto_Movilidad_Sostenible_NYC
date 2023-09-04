-- crear base de datos
CREATE DATABASE proyecto_ny;
use proyecto_ny;

CREATE TABLE Calidad_del_aire (
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

CREATE TABLE conta_sonora (
    fecha DATE,
    id_borough INT,
    engine_sounds INT,
    alert_signal_sounds INT,
    total_sounds INT,
    borough_name VARCHAR(255)
);

CREATE TABLE NYCCLIMA (
    time DATE,
    hours TIME,
    temperature FLOAT
);

CREATE TABLE Station (
    ID INT,
    Fuel_Type_Code VARCHAR(255),
    Station_Name VARCHAR(255),
    Street_Address VARCHAR(255),
    City VARCHAR(255),
    State VARCHAR(255),
    ZIP INT,
    Status_Code VARCHAR(255),
    Groups_With_Access_Code VARCHAR(255),
    Geocode_Status VARCHAR(255),
    Latitude FLOAT,
    Longitude FLOAT,
    Updated_At DATE,
    Country VARCHAR(255),
    Groups_With_Access_Code_French VARCHAR(255),
    Access_Code VARCHAR(255)
);

CREATE TABLE Taxi_zones (
    LocationID INT,
    Shape_Leng FLOAT,
    Shape_Area FLOAT,
    zone VARCHAR(255),
    borough VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT
);

CREATE TABLE taxiG (
    Fecha Date,
    Pasajeros_por_dia INT,
    Viajes_por_dia INT,
    Tarifario_por_dia FLOAT,
    Total_recaudado_por_dia FLOAT,
    Pago_con_tarjeta INT,
    Pago_con_efectivo INT,
    Tipo_de_Taxi VARCHAR(255)
);

CREATE TABLE taxiY (
    Fecha DATE,
    Pasajeros_por_dia INT,
    Viajes_por_dia INT,
    Tarifario_por_dia FLOAT,
    Total_recaudado_por_dia FLOAT,
    Pago_con_tarjeta INT,
    Pago_con_efectivo INT,
    Tipo_de_Taxi VARCHAR(255)
);

CREATE TABLE taxis_tarifa (
    Fecha DATE,
    Pasajeros_por_dia INT,
    Viajes_por_dia INT,
    Tarifario_por_dia FLOAT,
    Total_recaudado_por_dia FLOAT,
    Pago_con_tarjeta INT,
    Pago_con_efectivo INT,
    Tipo_de_Taxi VARCHAR(255)
);

CREATE TABLE veh_com (
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

show tables;