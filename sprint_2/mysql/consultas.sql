-- crear base de datos
CREATE DATABASE proyecto_ny;
use proyecto_ny;

-- RUTA DONDE VAN LOS ARCHIVOS A INGESTAR
SELECT @@global.secure_file_priv;
SHOW VARIABLES LIKE "secure_file_priv";

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

-- INSERTAR DATOS DESDE UN CSV
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Calidad_del_aire.csv'
INTO TABLE Calidad_del_aire
FIELDS  TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' 
LINES	TERMINATED BY '\n' IGNORE 1 LINES 
(Name,Measure,Measure_Info,Geo_Type_Name,Geo_Join_ID,Geo_Place_Name,Time_Period,Start_Date,Data_Value);

select * from calidad_del_aire;

CREATE TABLE IF NOT EXISTS conta_sonora (
    fecha DATE,
    id_borough INT,
    engine_sounds INT,
    alert_signal_sounds INT,
    total_sounds INT,
    borough_name VARCHAR(255)
);

-- INSERTAR DATOS DESDE UN CSV
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\conta_sonora.csv'
INTO TABLE conta_sonora
FIELDS  TERMINATED BY ',' ENCLOSED BY '' ESCAPED BY ''
LINES	TERMINATED BY '\n' IGNORE 1 LINES 
(fecha,id_borough,engine_sounds,alert_signal_sounds,total_sounds,borough_name);

SELECT * from conta_sonora LIMIT 4;

CREATE TABLE IF NOT EXISTS NYCCLIMA (
    time DATE,
    hours TIME,
    temperature FLOAT
);

-- INSERTAR DATOS DESDE UN CSV
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\NYCCLIMA.csv'
INTO TABLE NYCCLIMA
FIELDS  TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' 
LINES	TERMINATED BY '\n' IGNORE 1 LINES 
(time,hours,temperature);

select * from NYCCLIMA LIMIT 5;

CREATE TABLE IF NOT EXISTS Station_NY (
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
    Country VARCHAR(255),
    Groups_With_Access_Code_French VARCHAR(255),
    Access_Code VARCHAR(255)
);

-- INSERTAR DATOS DESDE UN CSV
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Station_NY.csv'
INTO TABLE Station_NY
FIELDS  TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' 
LINES	TERMINATED BY '\n' IGNORE 1 LINES 
(ID,Fuel_Type_Code,Station_Name,Street_Address,City,State,ZIP,Status_Code,Groups_With_Access_Code,Geocode_Status,Latitude,Longitude,Country,Groups_With_Access_Code_French,Access_Code);

SELECT * from Station_NY LIMIT 4;

CREATE TABLE IF NOT EXISTS Taxi_zones (
    LocationID INT,
    Shape_Leng FLOAT,
    Shape_Area FLOAT,
    zone VARCHAR(255),
    borough VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT
);

-- INSERTAR DATOS DESDE UN CSV
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Taxi_zones.csv'
INTO TABLE Taxi_zones
FIELDS  TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' 
LINES	TERMINATED BY '\n' IGNORE 1 LINES 
(LocationID,Shape_Leng,Shape_Area,zone,borough,latitude,longitude);

SELECT * from Taxi_zones limit 5;

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

-- INSERTAR DATOS DESDE UN CSV
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\taxiG.csv'
INTO TABLE taxiG
FIELDS  TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' 
LINES	TERMINATED BY '\n' IGNORE 1 LINES 
(Fecha,Pasajeros_por_dia,Viajes_por_dia,Tarifario_por_dia,Total_recaudado_por_dia,Pago_con_tarjeta,Pago_con_efectivo,Tipo_de_Taxi);

select * from taxiG limit 5;

CREATE TABLE IF NOT EXISTS taxiY (
    Fecha DATE,
    Pasajeros_por_dia INT,
    Viajes_por_dia INT,
    Tarifario_por_dia FLOAT,
    Total_recaudado_por_dia FLOAT,
    Pago_con_tarjeta INT,
    Pago_con_efectivo INT,
    Tipo_de_Taxi VARCHAR(255)
);

-- INSERTAR DATOS DESDE UN CSV
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\taxiY.csv'
INTO TABLE taxiY
FIELDS  TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' 
LINES	TERMINATED BY '\n' IGNORE 1 LINES 
(Fecha,Pasajeros_por_dia,Viajes_por_dia,Tarifario_por_dia,Total_recaudado_por_dia,Pago_con_tarjeta,Pago_con_efectivo,Tipo_de_Taxi);

select * from taxiY limit 5;

CREATE TABLE IF NOT EXISTS taxis_tarifa (
    Fecha DATE,
    Pasajeros_por_dia INT,
    Viajes_por_dia INT,
    Tarifario_por_dia FLOAT,
    Total_recaudado_por_dia FLOAT,
    Pago_con_tarjeta INT,
    Pago_con_efectivo INT,
    Tipo_de_Taxi VARCHAR(255)
);

-- INSERTAR DATOS DESDE UN CSV
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\taxis_tarifa.csv'
INTO TABLE taxis_tarifa
FIELDS  TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' 
LINES	TERMINATED BY '\n' IGNORE 1 LINES 
(Fecha,Pasajeros_por_dia,Viajes_por_dia,Tarifario_por_dia,Total_recaudado_por_dia,Pago_con_tarjeta,Pago_con_efectivo,Tipo_de_Taxi);

select * from taxis_tarifa;

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

-- INSERTAR DATOS DESDE UN CSV
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\veh_com.csv'
INTO TABLE veh_com
FIELDS  TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' 
LINES	TERMINATED BY '\n' IGNORE 1 LINES 
(Model_Year,Make,Model_1,Vehicle_Class,Engine_Size,Cylinders,Transmission,Fuel_Type,Fuel_Consumption_City,CO2_Emissions,CO2_Rating,Smog_Rating);

select * from veh_com;

show tables;