import streamlit as st
import pandas as pd
import joblib

model_filename = r'E:\000-USUARIOS\Pablo\Documentos\Documentos\Marce\Data y Machine Learning\HENRY\Proy. Final\Modelo\modelo_random_forest.joblib'# Cargo el modelo entrenado
loaded_model = joblib.load(model_filename)

promedio_pasajeros = 147049.8586065574 # Promedio de pasajeros

def predict_trips(avg_temperature, selected_dia_semana, tipo_taxi):
    dias_semana = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado']
    dia_semana_encoded = [0] * 6
    dia_semana_encoded[dias_semana.index(selected_dia_semana)] = 1
    tipo_taxi_encoded = 1 if tipo_taxi == 'yellow' else 0


    input_data = {
    'Pasajeros por dia': [promedio_pasajeros],
    'avg_temperature': [avg_temperature],
    'Tipo de Taxi_green': [0],  
    'Tipo de Taxi_yellow': [1], 
    'DiaSemana_Jueves': [0],  
    'DiaSemana_Lunes': [0],  
    'DiaSemana_Martes': [0],  
    'DiaSemana_Miércoles': [1],  
    'DiaSemana_Sábado': [0], 
    'DiaSemana_Viernes': [0] 
    }


    input_df = pd.DataFrame(input_data)

    prediction = loaded_model.predict(input_df)# Realizo predicción

    return prediction

st.title('Predicción de Cantidad de Viajes en Taxi por día')# Configuro Streamlit
st.write('Esta aplicación predice la cantidad de viajes en taxi por día en la ciudad de Nueva York.')

# Entrada de datos del usuario
avg_temperature = st.slider('Temperatura Promedio del día (°C)', min_value=-10.0, max_value=40.0, step=0.1, value=20.0)
dia_semana = st.selectbox('Día de la Semana', ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado'])
tipo_taxi = st.selectbox('Tipo de Taxi', ['green', 'yellow'])

# Realizo predicción
if st.button('Realizar Predicción'):
    prediction = predict_trips(avg_temperature, dia_semana, tipo_taxi)
    st.write(f'La cantidad estimada de viajes en taxi es: {prediction[0]:.2f}')

# Información adicional
st.write('Este modelo utiliza Random Forest para hacer predicciones. Los datos de entrada incluyen la temperatura promedio, el día de la semana y el tipo de taxi. La cantidad de pasajeros se mantiene como el promedio.')

# Texto de pie
st.write('Hecho por [María Marcela Balzarelli, Pablo Barchiesi, Jorgelina Ramos, Michael Martinez]')



