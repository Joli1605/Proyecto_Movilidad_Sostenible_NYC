
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

- María Marcela Balzarelli - Data Science

- Pablo Nahuel Barchiesi Ponce - Data Engineer

- Michael  Martinez Chinchilla - Data Engineer

- Jorgelina Paola Lujan Ramos - Data Analyst

## **Stack tecnológico**

Para llevar a cabo nuestro proyecto hemos seleccionado las siguientes tecnologías:

- Trabajo diario: python, google meet, github.

- Ingeniería de datos: Python, mysql.

- Análisis y visualización de datos: Power Bi, python.

- Modelo de machine learning: Python.

- Gestión de proyectos: Jira


## Solución data pipeline

En esta sección se estructurará el flujo de datos desde la recepción hasta la salida del ETL.

<br>
<div style="text-align: center;">
  <img src='./Images/pipeline.png' alt="imagen data pipeline">
</div>
<br>

### Ingesta de Datos: 

Los archivos de datos en los formatos CSV, Parquet y DBF se pueden cargar localmente al sistema. Estos archivos contienen información esencial sobre la movilidad urbana y son la base de nuestro análisis.

### Orquestación con Apache Airflow:

Utilizamos Apache Airflow para gestionar el flujo de trabajo de ingesta y procesamiento de datos. Airflow permite la programación automatizada de tareas y garantiza que los datos se manejen en el momento adecuado y en el orden correcto.

### Proceso ETL Automatizado: 

El proceso de Extracción, Transformación y Carga (ETL) se lleva a cabo utilizando scripts de Python. Estos scripts realizan la limpieza, transformación y enriquecimiento de los datos para prepararlos para su análisis.

### Almacenamiento en MySQL y Contenedor Docker: 

Los datos procesados se cargan en una base de datos MySQL. Para garantizar la portabilidad y el aislamiento, todo el sistema está contenido en un entorno Docker. Esto facilita la configuración y despliegue en diferentes entornos.

### Análisis de Datos en Power BI: 

Una vez que los datos se encuentran en la base de datos, se pueden analizar y visualizar utilizando Power BI. Esto permite identificar tendencias, patrones y obtener información valiosa para la toma de decisiones informadas.

### Modelo de Machine Learning y Streamlit:

Además del análisis tradicional, estamos construyendo un modelo de Machine Learning en Python. Este modelo se implementará en una aplicación interactiva utilizando Streamlit, lo que permitirá a los usuarios interactuar con el modelo y obtener predicciones en tiempo real.

## Indicadores Clave de Desempeño (KPIs)
En este proyecto, es esencial medir el desempeño y los resultados obtenidos a través de indicadores clave. Los siguientes KPIs se utilizarán para evaluar el éxito de nuestras soluciones y el impacto de la implementación de vehículos eléctricos en la flota de transporte:

### **KPI: Crecimiento Porcentual de Tarifas de Taxi Verdes dos años**

Objetivo: Evaluar si hubo un aumento de al menos el 5% en las tarifas entre los dos últimos años.

### **KPI: Reducción de Contaminación Sonora entre Vehículos de Combustión y Eléctricos**
 Objetivo: Demostrar una disminución del 10% en el nivel porcentual de ruido generado por vehículos en los dos ultimos años analizados.

### **KPI: Mes con Mayor Demanda**
Objetivo: Identificar el mes con la mayor cantidad de viajes realizados en los meses en los que se registra una mayor cantidad de lluvia en los últimos dos años 

### **KPI: Análisis de Demanda de Viajes por Boroughs**

Objetivo: Equilibrar la cantidad de viajes en los diferentes boroughs de la ciudad de Nueva York, buscando reducir la diferencia entre el borough con la cantidad máxima de viajes y el borough con la cantidad mínima, logrando una diferencia no mayor al 15%.

