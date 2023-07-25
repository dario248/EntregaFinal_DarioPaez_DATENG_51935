# Este script está pensado para correr en Spark y hacer el proceso de ETL de la tabla users

import requests
import argparse
import pandas as pd
import datetime as dt
from time import sleep
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from sqlalchemy.types import DECIMAL, VARCHAR

from commons import ETL_Pandas


def api_call(url: str, api_type: str) -> dict:

    print(f">>> [E] Extrayendo datos de la API {api_type}...")
    headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'
        }
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()["data"]
        print(data)
    else:
        print("Error al extraer datos de la API")
        data = []
        raise Exception("Error al extraer datos de la API")
    
    return data

def api_call_2(url: str, api_type: str) -> dict:
    print(f">>> [E] Extrayendo datos de la API {api_type}...")
    response = None
    while response is None:
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()["data"]
                print(data)
            break
        except requests.ConnectionError as e:
            print('Connection error occurred', e)
            sleep(1.5)
            continue
        except requests.Timeout as e:
            print('Timeout error - request took too long', e)
            sleep(1.5)
            continue
        except requests.RequestException as e:
            print('General error', e)
            sleep(1.5)
            continue
        except KeyboardInterrupt:
            print('The program has been canceled')

    return data

def df_a_mensual(dataframe: pd.DataFrame) -> pd.DataFrame:
    # toma la columna date de un dataframe y convierte el tipo de dato y lo pasa a periodo mensual
    dataframe_mensual = dataframe.copy()
    dataframe_mensual['date'] = [dt.datetime.strptime(fecha, '%Y-%m-%d') for fecha in dataframe['date']]
    dataframe_mensual['date'] = dataframe_mensual['date'].dt.to_period('M')

    return dataframe_mensual


def date_to_string(dataframe: pd.DataFrame) -> pd.DataFrame:
    # Convierte las columnas de fecha de un dataframe en strings
    dataframe['date'] = dataframe['date'].astype('string')
    dataframe['process_date'] = dataframe['process_date'].astype('string')

    return dataframe


class ETL_Final(ETL_Pandas):
    def __init__(self, dolar_variation_limit: float, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")
        self.url_tipo_cambio = "https://apis.datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26&limit=5000&start_date=2018-07&format=json"
        self.url_ipc = "https://apis.datos.gob.ar/series/api/series/?ids=148.3_INUCLEONAL_DICI_M_19&limit=5000&format=json"
        self.dolar_variation_limit = dolar_variation_limit

    def run(self):
        self.execute(self.process_date)

    def extract(self) -> dict:
        """
        Extrae datos de la API
        """
        # API está con fallos
        data_tipo_cambio = api_call(self.url_tipo_cambio, "Tipo de Cambio")
        data_ipc = api_call(self.url_ipc, "Indice de Precios")
        
        print(">>> [E] Creando Dataframes de Pandas...")
        
        df_tipo_cambio = pd.DataFrame(data_tipo_cambio, columns=['date', 'tipo_cambio_bna_vendedor'])
        df_ipc = pd.DataFrame(data_ipc, columns=['date', 'ipc'])

        # df_tipo_cambio = pd.read_sql('tipo_de_cambio', con=self.pyscopg_connection)
        # df_ipc = pd.read_sql('ipc_nacional', con=self.pyscopg_connection)
        df_tipo_cambio.info()
        df_ipc.info()

        dataframes = {'tipo_de_cambio': df_tipo_cambio,
                      'ipc': df_ipc}
        
        return dataframes

    def transform(self, dfs_original: dict) -> dict:
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")
        
        # Conversión de string a datetime y paso a período mensual
        df_tipo_cambio_mensual = df_a_mensual(dfs_original['tipo_de_cambio'])
        df_ipc_mensual = df_a_mensual(dfs_original['ipc'])

        # Agrupar por mes, calcular el promedio de tipo de cambio y ordenar de forma ascendente
        df_cambio_mensual = df_tipo_cambio_mensual.groupby(['date']).mean()

        # Comparación de IPC con media de tipo de cambio vendedor
        df_analisis = pd.merge(df_cambio_mensual, df_ipc_mensual, on='date')
        df_analisis['date'] = df_analisis['date'].astype('object')
        df_analisis.info()

        # Adjunto el nuevo dataframe al diccionario de dataframes 
        dfs_original['analisis'] = df_analisis

        return dfs_original

    def load(self, df_final: dict) -> None:
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")
        # Extraer dataframes del diccionario
        df_tipo_cambio = df_final['tipo_de_cambio']
        df_ipc = df_final['ipc']
        df_analisis = df_final['analisis']

        # Agregar columna process_date
        df_tipo_cambio['process_date'] = self.process_date
        df_ipc['process_date'] = self.process_date
        df_analisis['process_date'] = self.process_date

        # Convertir tipo de datos fecha en strings
        df_tipo_cambio = date_to_string(df_tipo_cambio)
        df_ipc = date_to_string(df_ipc)
        df_analisis = date_to_string(df_analisis)
        
        # Cargar datos de las tres tablas en Redshift
        df_tipo_cambio.to_sql('tipo_de_cambio', 
                              con=self.pyscopg_connection, 
                              if_exists='append', 
                              index=False, 
                              schema="paezdario24_coderhouse",
                              dtype={'date': VARCHAR(10),
                                     'tipo_cambio_bna_vendedor': DECIMAL(5,2),
                                     'process_date': VARCHAR(10)})
        
        df_ipc.to_sql('ipc_nacional', 
                      con=self.pyscopg_connection, 
                      if_exists='append', 
                      index=False, 
                      schema="paezdario24_coderhouse",
                      dtype={'date': VARCHAR(10), 
                             'ipc': DECIMAL(7,2), 
                             'process_date': VARCHAR(10)})
        
        df_analisis.to_sql('analisis_economico', 
                           con=self.pyscopg_connection, 
                           if_exists='append', 
                           index=False, 
                           schema="paezdario24_coderhouse",
                           dtype={'date': VARCHAR(10), 
                                  'ipc': DECIMAL(7,2),
                                  'tipo_cambio_bna_vendedor': DECIMAL(5,2), 
                                  'process_date': VARCHAR(10)})
        
        print(">>> [L] Datos cargados exitosamente")
        print(">>> [L] Chequeando variaciones del dólar")
        dolar_flag = self.check_tipo_cambio(df_tipo_cambio)
        print(dolar_flag)

    def check_tipo_cambio(self, df_tipo_cambio: pd.DataFrame) -> int:
        # chequea si hay variaciones bla del dolar bla

        # Filtrar a los últimos 20 días
        df_filtered = df_tipo_cambio.iloc[-20:, :].reset_index(drop=True)

        # Generar serie de variacion
        variaciones = []
        for i in df_filtered.index:
            if i == 0:
                variaciones.append(0)
            else:
                variacion = round(((df_filtered['tipo_cambio_bna_vendedor'][i] / df_filtered['tipo_cambio_bna_vendedor'][i - 1]) - 1), 3)
                variaciones.append(variacion)

        serie_variaciones = pd.Series(variaciones)
        dolar_flag = (serie_variaciones >= self.dolar_variation_limit).sum()

        return dolar_flag

if __name__ == "__main__":
    print("Corriendo script")
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
                "dolar_variation_limit", type=float, help="Dolar Variation limit to be considered important"
            )
    args = parser.parse_args()
    etl = ETL_Final(dolar_variation_limit=args.dolar_variation_limit)
    etl.run()