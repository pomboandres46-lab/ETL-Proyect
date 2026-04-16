from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd

import os

# ---------------------------------------------------------
# 1. CONFIGURACIÓN DE RUTAS (Basado en Codespaces)
# ---------------------------------------------------------
# Usamos la variable de entorno de Airflow, o el directorio por defecto del contenedor
BASE_PATH = os.environ.get('AIRFLOW_HOME', '/opt/airflow')

yahoo_data = f"{BASE_PATH}/data/yahoo.csv"
Temp_path = f"{BASE_PATH}/temp"

#1. Definir argumentos por defecto
default_args = {
     'owner': 'data_enginner',
     'depends_on_past': False,
     'retries': 1,
     'retry_delay': timedelta(seconds=15),
     }


def extraccion():
    print(f" Extrayendo datos de Yahoo Finance...")

    if not os.path.exists(yahoo_data):
        raise FileNotFoundError(f"El archivo {yahoo_data} no existe")
    
    try:
        data = pd.read_csv(yahoo_data)
        data.to_csv(f"{Temp_path}/yahoo.csv", index=False)
        print(f" Datos extraidos correctamente")
    except Exception as e:
        print(f" Error al extraer los datos: {e}")

def transformacion():
    print(f" Transformando datos...")
    
    try:
        data = pd.read_csv(f"{Temp_path}/yahoo.csv")
        data = pd.DataFrame(data)
        # Formateamos columnas
        data.columns = [f"{col[0]}_{col[1]}" for col in data.columns]
        symbol = data.columns[0].split("_")[1]
        data = data.rename(columns={
        f'Close_{symbol}': 'close',
        f'Open_{symbol}': 'open',
        f'High_{symbol}': 'high',
        f'Low_{symbol}': 'low',
        f'Volume_{symbol}': 'volume'
        })
        data = data.reset_index().rename(columns={'index':'timestamp', 'Datetime':'timestamp', 'Date':'timestamp'})
        data['symbol'] = symbol
        data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True)    
        data["timestamp"] = data["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
        data = data[['timestamp','symbol','open','high','low','close']]
        data.to_csv(f"{Temp_path}/yahoo_transformado.csv", index=False)
        print(f" Datos transformados correctamente")
    except Exception as e:
        print(f" Error al transformar los datos: {e}")

with DAG(
    'ETL_Delivery2',
    default_args=default_args,
    description='ETL_Delivery2',
    schedule_interval=None, # Changed schedule_interval for easier testing
    start_date=datetime(2026, 4, 16),
    catchup=False,
    tags=['Delivery2'],
    ) as dag:     

        #Tarea de extraccion
        extraccion_task = PythonOperator(
            task_id='extraccion',
            python_callable=extraccion,
        )

        transformacion_task = PythonOperator(
            task_id='transformacion',
            python_callable=transformacion,
        )

extraccion_task >> transformacion_task
        
