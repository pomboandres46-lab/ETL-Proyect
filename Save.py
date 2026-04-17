

import pandas as pd
import os



# 2. Configuración de rutas


def Guarda(Fuente, data):
    FOLDER_PATH = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    CSV_FILE_YAHOO = f"{FOLDER_PATH}/data/yahoo.csv"
    CSV_FILE_FINHUB = f"{FOLDER_PATH}/data/stream.csv"
    CSV_FILE_ALPHA = f"{FOLDER_PATH}/data/stream.csv"

    if Fuente == "Yahoo":

        try:
            
            df_new = pd.DataFrame(data)
            print("Ejecutando Guardado yahoo...")

            # Guardado en modo Append
            archivo_existe = os.path.isfile(CSV_FILE_YAHOO)
            df_new.to_csv(CSV_FILE_YAHOO, mode='a', index=False, header=not archivo_existe)

            print("Datos de Yahoo Guardados...")

        except Exception as e:
            print(f"Error al guardar los datos Yahoo: {e}")

    elif Fuente == "Finhub":
        try:
            df_new = pd.DataFrame(data['data'])

            # Guardado en modo Append
            archivo_existe = os.path.isfile(CSV_FILE_FINHUB)
            df_new.to_csv(CSV_FILE_FINHUB, mode='a', index=False, header=not archivo_existe)

            print(f"✅ Guardado finhub.")

        except Exception as e:
            print(f"Error al guardar los datos finhub: {e}")

    elif Fuente == "Alpha":
        try:
            info = data['Realtime Currency Exchange Rate']
            data_new = pd.DataFrame(info)

            # Guardado en modo Append
            archivo_existe = os.path.isfile(CSV_FILE_ALPHA)
            data_new.to_csv(CSV_FILE_ALPHA, mode='a', index=False, header=not archivo_existe)

            print(f"✅ Guardado alpha.")

        except Exception as e:
            print(f"Error al guardar los datos alpha: {e}")

