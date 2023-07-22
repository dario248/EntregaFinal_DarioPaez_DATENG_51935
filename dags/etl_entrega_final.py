# Este es el DAG que orquesta el ETL de la tabla users
import pandas as pd
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


from airflow.models import Variable

from datetime import datetime, timedelta

QUERY_CREATE_TABLE_CAMBIO = """
CREATE TABLE IF NOT EXISTS tipo_de_cambio (
    date VARCHAR(10),
    tipo_cambio_bna_vendedor decimal(5,2),
    process_date VARCHAR(10) distkey
) SORTKEY(process_date, date);
"""

QUERY_CREATE_TABLE_IPC = """
CREATE TABLE IF NOT EXISTS ipc_nacional (
    date VARCHAR(10),
    ipc decimal(7,2),
    process_date VARCHAR(10) distkey
) SORTKEY(process_date, date);
"""

QUERY_CREATE_TABLE_ANALISIS = """
CREATE TABLE IF NOT EXISTS analisis_economico (
    date VARCHAR(10),
    ipc decimal(7,2),
    tipo_cambio_bna_vendedor decimal(5,2),
    process_date VARCHAR(10) distkey
) SORTKEY(process_date, date);
"""

QUERY_CLEAN_PROCESS_DATE = """
DELETE FROM analisis_economico WHERE process_date = '{{ ti.xcom_pull(key="process_date") }}';
"""


# create function to get process_date and push it to xcom
def get_process_date(**kwargs):
    # If process_date is provided take it, otherwise take today
    if (
        "process_date" in kwargs["dag_run"].conf
        and kwargs["dag_run"].conf["process_date"] is not None
    ):
        process_date = kwargs["dag_run"].conf["process_date"]
    else:
        process_date = kwargs["dag_run"].conf.get(
            "process_date", datetime.now().strftime("%Y-%m-%d")
        )
    kwargs["ti"].xcom_push(key="process_date", value=process_date)


defaul_args = {
    "owner": "Dario paez",
    "start_date": datetime(2023, 7, 20),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="etl_entrega_final",
    default_args=defaul_args,
    description="ETL de la tablas de índices económicos",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Tareas
    get_process_date_task = PythonOperator(
        task_id="get_process_date",
        python_callable=get_process_date,
        provide_context=True,
        dag=dag,
    )

    create_table_cambio = SQLExecuteQueryOperator(
        task_id="create_table_cambio",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE_CAMBIO,
        dag=dag,
    )

    create_table_ipc = SQLExecuteQueryOperator(
        task_id="create_table_ipc",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE_IPC,
        dag=dag,
    )

    create_table_analisis = SQLExecuteQueryOperator(
        task_id="create_table_analisis",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE_ANALISIS,
        dag=dag,
    )

    pandas_etl = BashOperator(
        task_id="pandas_etl",
        bash_command=f'python {Variable.get("python_scripts_dir")}/etl_pandas.py',
        dag=dag,
    )

    get_process_date_task >> create_table_cambio >> create_table_ipc >> create_table_analisis >> pandas_etl