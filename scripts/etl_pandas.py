# Script que define el ETL principal utilizando Pandas

import requests
import argparse
import pandas as pd
import datetime as dt
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from sqlalchemy.types import DECIMAL, VARCHAR
from commons import ETL_Pandas


class ETL_Final(ETL_Pandas):
    """
    Clase para ejecutar el ETL principal

    Attributes
    ----------
    dolar_variation_limit: float
        Umbral de variacion del precio del dolar para considerar que es un cambio importante
    job_name: str
        Nombre de la ejecución del proceso ETL

    Methods
    -------
    api_call()
        Realiza un pedido de datos a una determinada API
    """
    def __init__(self, dolar_variation_limit: float, job_name: str=None) -> None:
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")
        self.url_tipo_cambio = "https://apis.datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26&limit=5000&start_date=2018-07&format=json"
        self.url_ipc = "https://apis.datos.gob.ar/series/api/series/?ids=148.3_INUCLEONAL_DICI_M_19&limit=5000&format=json"
        self.dolar_variation_limit = dolar_variation_limit


    def run(self):
        """
        Ejecuta ETL completo
        """
        self.execute(self.process_date)


    def api_call(self, url: str, api_type: str) -> dict:
        """
        Realiza el request a la API dado un determinado url

        Parameters
        ----------
        url: str
            Dirección url de la API que se quiere consultar
        api_type: str
            Especifica a que tipo de API se quiere conectar ['Tipo de Cambio', 'Indice de Precios']

        Returns
        -------
        dict:
            Diccionario que contiene los datos extraidos de la API
        """
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
    

    def _extract(self) -> dict:
        """
        Extrae tablas de dos APIs y los guarda en dos dataframes de Pandas

        Returns
        -------
        dict
            Diccionario que contiene los dos dataframes 'tipo_de_cambio' e 'ipc'
        """
        # Llamada a las APIs
        data_tipo_cambio = self.api_call(self.url_tipo_cambio, "Tipo de Cambio")
        data_ipc = self.api_call(self.url_ipc, "Indice de Precios")
        
        print(">>> [E] Creando Dataframes de Pandas...")
        
        df_tipo_cambio = pd.DataFrame(data_tipo_cambio, columns=['date', 'tipo_cambio_bna_vendedor'])
        df_ipc = pd.DataFrame(data_ipc, columns=['date', 'ipc'])

        df_tipo_cambio.info()
        df_ipc.info()

        dataframes = {'tipo_de_cambio': df_tipo_cambio,
                      'ipc': df_ipc}
        
        return dataframes


    def _transform(self, dfs_original: dict) -> dict:
        """
        Transforma los datos extraídos de las APIs

        Parameters
        ----------
        dfs_original: pd.DataFrame
            Diccionario que contiene los dataframes originales

        Returns
        -------
        dict:
            Diccionario que contiene los dfs extraídos y un dataframe de análisis
        """
        print(">>> [T] Transformando datos...")
        
        # Conversión de string a datetime y paso a período mensual
        df_tipo_cambio_mensual = self._df_a_mensual(dfs_original['tipo_de_cambio'])
        df_ipc_mensual = self._df_a_mensual(dfs_original['ipc'])

        # Agrupar por mes, calcular el promedio de tipo de cambio y ordenar de forma ascendente
        df_cambio_mensual = df_tipo_cambio_mensual.groupby(['date']).mean()

        # Comparación de IPC con media de tipo de cambio vendedor
        df_analisis = pd.merge(df_cambio_mensual, df_ipc_mensual, on='date')
        df_analisis['date'] = df_analisis['date'].astype('object')
        df_analisis.info()

        # Adjunto el nuevo dataframe al diccionario de dataframes 
        dfs_original['analisis'] = df_analisis

        return dfs_original


    def _load(self, df_final: dict) -> None:
        """
        Carga los datos en la base de datos de Redshift

        Parameters
        ----------
        df_final: dict
            Diccionario con los dataframes finales que van a ser cargados en la base de datos
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
        df_tipo_cambio = self._date_to_string(df_tipo_cambio)
        df_ipc = self._date_to_string(df_ipc)
        df_analisis = self._date_to_string(df_analisis)
        
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
        dolar_flag = self._check_tipo_cambio(df_tipo_cambio)
        print(dolar_flag)


    def _check_tipo_cambio(self, df_tipo_cambio: pd.DataFrame) -> int:
        """
        Analiza las variaciones de precio del dolar de los últimos 20 dias

        Parameters
        ----------
        df_tipo_cambio: pd.DataFrame
            DataFrame con precios de tipo de cambio vendedor del BNA

        Returns
        -------
        dolar_flag: int
            Valor que indica en cuantos dias se encontraron variaciones de precio importantes
        """ 
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
        # Obtener en cuantos registros la variacion supera el umbral definido
        dolar_flag = (serie_variaciones >= self.dolar_variation_limit).sum()

        return dolar_flag


    def _df_a_mensual(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Toma la columna 'date' de un dataframe y convierte el tipo de dato y lo pasa a periodo mensual

        Parameters
        ----------
        dataframe: pd.DataFrame
            DataFrame a procesar

        Returns
        -------
        pd.DataFrame:
            DataFrame resultante con columna 'date' en período mensual
        """
        dataframe_mensual = dataframe.copy()
        dataframe_mensual['date'] = [dt.datetime.strptime(fecha, '%Y-%m-%d') for fecha in dataframe['date']]
        dataframe_mensual['date'] = dataframe_mensual['date'].dt.to_period('M')

        return dataframe_mensual
    

    def _date_to_string(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Convierte las columnas relacionadas a fechas en tipo 'string'

        Parameters
        ----------
        dataframe: pd.DataFrame
            DataFrame a procesar

        Returns
        -------
        pd.DataFrame:
            DataFrame resultante con columnas de fechas en formato 'string'
        """
        dataframe['date'] = dataframe['date'].astype('string')
        dataframe['process_date'] = dataframe['process_date'].astype('string')

        return dataframe
    

if __name__ == "__main__":
    print("Corriendo script")
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
                "dolar_variation_limit", type=float, help="Dolar Variation limit to be considered important"
            )
    args = parser.parse_args()
    etl = ETL_Final(dolar_variation_limit=args.dolar_variation_limit)
    etl.run()