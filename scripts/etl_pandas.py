# Este script está pensado para correr en Spark y hacer el proceso de ETL de la tabla users

import requests
import pandas as pd
import datetime as dt
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta
from os import environ as env
from sqlalchemy.types import DECIMAL, VARCHAR

from commons import ETL_Pandas


def api_call(url: str, api_type: str) -> dict:

    print(f">>> [E] Extrayendo datos de la API {api_type}...")
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.get(url)

    if response.status_code == 200:
        data = response.json()["data"]
        print(data)
    else:
        print("Error al extraer datos de la API")
        data = []
        raise Exception("Error al extraer datos de la API")
    
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
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")
        self.url_tipo_cambio = "https://apis.datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26&limit=5000&start_date=2018-07&format=json"
        self.url_ipc = "https://apis.datos.gob.ar/series/api/series/?ids=148.3_INUCLEONAL_DICI_M_19&limit=5000&format=json"

    def run(self):
        self.execute(self.process_date)

    def extract(self) -> dict:
        """
        Extrae datos de la API
        """
        data_tipo_cambio = api_call(self.url_tipo_cambio, "Tipo de Cambio")
        data_ipc = api_call(self.url_ipc, "Indice de Precios")
        
        print(">>> [E] Creando Dataframes de Pandas...")
        
        df_tipo_cambio = pd.DataFrame(data_tipo_cambio, columns=['date', 'tipo_cambio_bna_vendedor'])
        df_ipc = pd.DataFrame(data_ipc, columns=['date', 'ipc'])

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
        
        # df_tipo_cambio.write \
        #     .format("jdbc") \
        #     .option("url", env['REDSHIFT_URL']) \
        #     .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.tipo_de_cambio") \
        #     .option("user", env['REDSHIFT_USER']) \
        #     .option("password", env['REDSHIFT_PASSWORD']) \
        #     .option("driver", "org.postgresql.Driver") \
        #     .mode("append") \
        #     .save()
        
        # df_ipc.write \
        #     .format("jdbc") \
        #     .option("url", env['REDSHIFT_URL']) \
        #     .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.ipc_nacional") \
        #     .option("user", env['REDSHIFT_USER']) \
        #     .option("password", env['REDSHIFT_PASSWORD']) \
        #     .option("driver", "org.postgresql.Driver") \
        #     .mode("append") \
        #     .save()
        
        # df_analisis.write \
        #     .format("jdbc") \
        #     .option("url", env['REDSHIFT_URL']) \
        #     .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.analisis_economico") \
        #     .option("user", env['REDSHIFT_USER']) \
        #     .option("password", env['REDSHIFT_PASSWORD']) \
        #     .option("driver", "org.postgresql.Driver") \
        #     .mode("append") \
        #     .save()
        
        print(">>> [L] Datos cargados exitosamente")


if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Final()
    etl.run()