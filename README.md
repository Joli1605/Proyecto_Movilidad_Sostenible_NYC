
<br>
<div style="text-align: center;">
  <img src='./Images/Logo.jpg' alt="Logo Consultora">
</div>
<br>

# Proyecto_Movilidad_Sostenible_NYC

## Entendimiento de la situación propuesta: 

La empresa de servicios de transporte de pasajeros desea invertir en el sector con automóviles en la ciudad de Nueva York. Para ello, contrata a Analytica Data Solutions como Consultores externos para encontrar soluciones innovadoras, en donde se estudiará la relación entre estos medios de transporte particulares, la calidad del aire,  la contaminación sonora y correlaciones climáticas, con el objetivo de considerar la posibilidad de implementar vehículos eléctricos en su flota. Se deberá realizar un análisis preliminar del movimiento de los taxis en la ciudad para obtener un marco de referencia y tomar decisiones bien fundamentadas.

El proyecto implica recopilar, depurar y disponibilizar información relevante de diferentes fuentes para analizar la relación entre el transporte de pasajeros con automóviles en Nueva York, la calidad del aire, la contaminación sonora y correlaciones climáticas. A través de reportes, dashboards y el entrenamiento de un modelo de machine learning de clasificación, se resolverá un problema específico relacionado con los objetivos del proyecto.


## Objetivos

- Recopilar y depurar datos de diferentes fuentes para crear una base de datos (DataWarehouse).
- Realizar un análisis exploratorio de los datos para encontrar relaciones
- Crear un dashboard interactivo y visualmente atractivo que integre los resultados del análisis exploratorio de datos
- Entrenar y poner en producción un modelo de machine learning de clasificación para resolver el problema de inversión en el sector.


## Roles y responsabilidades

María Marcela Balzarelli - Data Science

Pablo Nahuel Barchiesi Ponce - Data Engineer

Michael  Martinez Chinchilla - Data Engineer

Jorgelina Paola Lujan Ramos - Data Analyst

## **Stack tecnológico**

Para llevar a cabo nuestro proyecto hemos seleccionado las siguientes tecnologías:

Trabajo diario: python, google meet, github.

Ingeniería de datos: Microsoft Azure, Python, sql.

Análisis y visualización de datos: Power Bi, python.

Modelo de machine learning: Python.

Gestión de proyectos: Jira

## Solución data pipeline

En esta sección se estructurará el flujo del dato desde la recepción hasta la salida del ETL.

<br>
<div style="text-align: center;">
  <img src='./Images/data_pipeline.png' alt="imagen data pipeline">
</div>
<br>

### **Data Ingest**

Los datos entregados por la empresa y extraídos por nuestro equipo mediante api y web scraping se descargaron y son almacenados de manera temporal en el localhost de nuestra máquina.

Dado que trabajaremos sobre el esquema de Microsoft Azure se creará un contenedor donde se almacenarán los datasets sin procesar en la nube. Para esto, fue necesario crear una cuenta de trabajo en el portal de Azure. En dicha cuenta se crea un grupo de recursos donde incluímos una cuenta de almacenamiento con un contenedor.

### **Conexión con Databricks**

Una vez almacenados los datasets en el contenedor de Azure se procede a realizar la conexión con Databricks, nuestro lugar de trabajo principal.

En el grupo de recursos previamente creado se añade un workspace de Databricks. Ahí se creará un clúster que permite computar nuestros datos (Single Node 10.4 LTS Apache Spark 14 GB Memory, 4 Cores), el criterio de selección es en base al alcance de nuestros recursos.

Dentro de Databricks creamos un Notebook y lo conectamos con el clúster. En dicho Notebook establecemos las variables necesarias para la conexión con el contenedor.


### **ETL**
Se realizará todo el proceso de extracción, transformación y carga de los datos hacia el data warehouse

### **Conexión con SQL database**
Creada la SQL Database de Azure se realizará la conexión con Databricks por medio del protocolo jdbc.

### **Conexión con Power BI**

La conexión se realiza mediante el conector de Azure SQL Database de PowerBI. Se ingresan las credenciales del servidor de base de datos y se cargan los datos ya sea por Direct Query o Import Data.


