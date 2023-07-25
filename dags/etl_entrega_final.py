from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable

from datetime import datetime, timedelta
import smtplib


def success_callback_function(context: dict) -> None:
    """
    Callback para enviar mail en caso de exito en la ejecución del DAG

    Parameters
    ----------
    context: dict
        Diccionario con referencias a los objetos de la task
    """
    dag_run = context.get("dag_run")
    msg = "DAG ran successfully"
    subject = f"DAG {dag_run} has completed"
    send_email(msg, subject)


def failure_callback_function(context: dict) -> None:
    """
    Callback para enviar mail en caso de fallo en la ejecución del DAG

    Parameters
    ----------
    context: dict
        Diccionario con referencias a los objetos de la task
    """
    dag_run = context.get("dag_run")
    msg = "DAG ran failed"
    subject = f"DAG {dag_run} has failed"
    send_email(msg, subject)


def send_email(msg: str, subject: str) -> None:
    """
    Envia un mail con el mensaje especificado utilizando la configuracion de variables de airflow

    Parameters
    ----------
    msg: str
        Cuerpo del mail
    subject: str
        Asunto del mail
    """
    try:
        x = smtplib.SMTP("smtp.gmail.com", 587)
        x.starttls()
        x.login(Variable.get("SMTP_EMAIL_FROM"), Variable.get("SMTP_PASSWORD"))

        message = "Subject: {}\n\n{}".format(subject, msg)
        x.sendmail(
            Variable.get("SMTP_EMAIL_FROM"), Variable.get("SMTP_EMAIL_TO"), message
        )
        print("Exito al enviar el mail")
    except Exception as exception:
        print(exception)
        print("Fallo al enviar el mail")


def get_process_date(**kwargs):
    """
    Obtiene la fecha de proceso de la tarea y la guarda en XCOM
    """
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


def check_dolar_values(ti: dict) -> None:
    """
    Comprueba si la condicion de variacion del dolar se cumple para enviar un aviso por mail

    Parameters
    ----------
    ti: dict
        Diccionario con referencias a los objetos de la task
    """
    # Obtiene la cantidad de casos que han superado el umbral del return de la tarea pandas_etl
    dolar_flag = ti.xcom_pull(task_ids="pandas_etl")
    if dolar_flag >= Variable.get("DOLAR_FLAG_THRESHOLD"):
        msg = "Anda a comprar dolares"
        subject = "Posible aumento importante del dolar"
        send_email(msg, subject)


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
    on_success_callback=success_callback_function,
    on_failure_callback=failure_callback_function,
    template_searchpath=[f'{Variable.get("python_scripts_dir")}/'],
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
        sql="query_create_table_cambio.sql",
        dag=dag,
    )

    create_table_ipc = SQLExecuteQueryOperator(
        task_id="create_table_ipc",
        conn_id="redshift_default",
        sql="query_create_table_ipc.sql",
        dag=dag,
    )

    create_table_analisis = SQLExecuteQueryOperator(
        task_id="create_table_analisis",
        conn_id="redshift_default",
        sql="query_create_table_analisis.sql",
        dag=dag,
    )

    pandas_etl = BashOperator(
        task_id="pandas_etl",
        bash_command=f'python {Variable.get("python_scripts_dir")}/etl_pandas.py {Variable.get("dolar_variation_limit")}',
        dag=dag,
        do_xcom_push=True,
    )

    check_dolar_values_task = PythonOperator(
        task_id="check_dolar_values",
        python_callable=check_dolar_values,
        provide_context=True,
        dag=dag,
    )

    (
        get_process_date_task
        >> create_table_cambio
        >> create_table_ipc
        >> create_table_analisis
        >> pandas_etl
        >> check_dolar_values_task
    )
