import sqlalchemy
import pandas as pd
from os import environ as env
from psycopg2 import connect
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Variables de configuración de Redshift
REDSHIFT_HOST = env["REDSHIFT_HOST"]
REDSHIFT_PORT = env["REDSHIFT_PORT"]
REDSHIFT_DB = env["REDSHIFT_DB"]
REDSHIFT_USER = env["REDSHIFT_USER"]
REDSHIFT_PASSWORD = env["REDSHIFT_PASSWORD"]
REDSHIFT_URL = env["REDSHIFT_URL"]


class ETL_Pandas:
    """
    Clase base de un ETL ejecutado en Pandas

    Attributes
    ----------
    job_name: str
        Nombre de la ejecución del proceso ETL

    Methods
    -------
    execute()
        Realiza la ejecución de extract, transform y load
    """
    def __init__(self, job_name=None):
        """
        Constructor de la clase, inicializa la sesión de Spark
        """
        print(">>> [init] Inicializando ETL...")
        self.pyscopg_connection = self._alchemy_engine()
        try:
            # Conectar a Redshift
            print(">>> [init] Conectando a Redshift...")
            self.conn_redshift = connect(
                host=REDSHIFT_HOST,
                port=REDSHIFT_PORT,
                database=REDSHIFT_DB,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD,
            )
            self.cur_redshift = self.conn_redshift.cursor()
            print(">>> [init] Conexión exitosa")
            # Cerrar la conexión
            self.cur_redshift.close()
            self.conn_redshift.close()
        except:
            print(">>> [init] No se pudo conectar a Redshift")


    def _alchemy_engine(self) -> sqlalchemy.engine:
        """
        Crea un objeto de conexion con la base de datos de Redshift

        Returns
        -------
        sqlalchemy.engine:
            Objeto de conexión con una base de datos
        """
        url_object = URL.create(
            "postgresql+psycopg2",
            username=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            host=REDSHIFT_HOST,
            database=REDSHIFT_DB,
            port=REDSHIFT_PORT
        )
        connection = create_engine(url_object)

        return connection


    def execute(self, process_date: str):
        """
        Método principal que ejecuta el ETL

        Args:
            process_date (str): Fecha de proceso en formato YYYY-MM-DD
        """
        print(">>> [execute] Ejecutando ETL...")

        # Extraemos datos de la API
        df_api = self._extract()

        # Transformamos los datos
        df_transformed = self._transform(df_api)

        # Cargamos los datos en Redshift
        self._load(df_transformed)

    
    def _extract(self):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la API...")


    def _transform(self, df_original: pd.DataFrame):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")


    def _load(self, df_final: pd.DataFrame):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")