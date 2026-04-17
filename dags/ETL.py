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
finhub_data = f"{BASE_PATH}/data/finhub.csv"
alpha_data = f"{BASE_PATH}/data/alpha.csv"
Temp_path = f"{BASE_PATH}/data/temp"

#1. Definir argumentos por defecto
default_args = {
     'owner': 'data_enginner',
     'depends_on_past': False,
     'retries': 1,
     'retry_delay': timedelta(seconds=15),
     }

# ---------------------------------------------------------
# Declaramos la funcion de extraccion
# ---------------------------------------------------------

def extraccion():   
# Iniciamos extrayendo los datos por lotes, los cuales serán nuestra base o perfil de datos
    print(f" Extrayendo datos...")
    if not os.path.exists(yahoo_data):
        print("No se encontro el archivo yahoo.csv")  
    try:
        data_yahoo = pd.read_csv(yahoo_data)
        data_yahoo.to_csv(f"{Temp_path}/yahoo.csv", index=False)
        print(f" Datos extraidos correctamente")
    except Exception as e:
        print(f" Error al extraer los datos: {e}")
# Ahora extraemos los datos de finhub
    try:
        if not os.path.exists(finhub_data):
            raise FileNotFoundError("No se encontro el archivo finhub.csv")
        data_finhub = pd.read_csv(finhub_data)
        data_finhub.to_csv(f"{Temp_path}/finhub.csv", index=False)
        print(f" Datos extraidos correctamente")
    except Exception as e:
        print(f" Error al extraer los datos: {e}")
# Ahora extraemos los datos de alpha
    try:
        if not os.path.exists(alpha_data):
            raise FileNotFoundError("No se encontro el archivo alpha.csv")
        data_alpha = pd.read_csv(alpha_data)
        data_alpha.to_csv(f"{Temp_path}/alpha.csv", index=False)
        print(f" Datos extraidos correctamente")
    except Exception as e:
        print(f" Error al extraer los datos: {e}")

# ---------------------------------------------------------
# Declaramos la funcion de transformacion
# ---------------------------------------------------------

def transformacion():
    print(f" Transformando datos de yahoo...")
    if os.path.exists(f"{Temp_path}/yahoo.csv"):
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
            os.remove(f"{Temp_path}/yahoo.csv")
            print(f" Datos transformados correctamente")
        except Exception as e:
            print(f" Error al transformar los datos: {e}")

    if os.path.exists(f"{Temp_path}/finhub.csv"):
        print(f" Transformando datos de finhub...")
        try:
            data = pd.read_csv(f"{Temp_path}/finhub.csv")
            data = pd.DataFrame(data)
            
            data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)

            # Agrupar por minuto y calcular OHLC
            ohlc = (
                data.groupby(["symbol", data["timestamp"].dt.floor("min")])["price"]
                .agg(open="first", high="max", low="min", close="last")
                .reset_index()
            )
            # Renombrar columnas para claridad
            ohlc = ohlc.rename(columns={"timestamp": "minute"})

            ohlc["timestamp"] = ohlc["minute"]

            data = ohlc[["timestamp", "symbol", "open", "high", "low", "close"]]

            data.to_csv(f"{Temp_path}/finhub_transformado.csv", index=False)
            os.remove(f"{Temp_path}/finhub.csv")
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
        
