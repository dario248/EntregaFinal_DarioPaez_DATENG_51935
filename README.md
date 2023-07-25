# CoderHouse - Data Engineering - Comisión 51935

## Entrega Final - Darío Páez

## Estructura del Repositorio
- `docker-compose.yml` -> contiene la configuración para montar el contenedor de **Docker**
- `.env` -> Archivo de variables de entorno. Contiene variables de conexión a Redshift y driver de Postgres.
- `docker_images/airflow/` -> contiene un archivo `DockerFile` para crear la imagen de **Airflow** y el archivo `requirements.txt` con las dependencias necesarias de la imagen
- `dags` -> carpeta con los scripts de los DAGs a correr
  -  `etl_entrega_final.py` contiene el DAG completo a ejecutar
- `scripts` -> carpeta con scripts python y sql
  -   `commons.py`: script con funciones base para un ETL hecho en pandas
  -   `etl_pandas.py`: script con ETL principal hecho con pandas
  -   `postgresql-42.5.2.jar`: driver de Postgres
  -   `query_create_table_analisis.sql`, `query_create_table_cambio.sql`, `query_create_table_ipc.sql`: queries sql para crear las tablas necesarias en la base de datos **Redshift**

## Set-Up
### Generar la imagen de Airflow
- Posicionarse en la carpeta `docker_images/airflow/`
- Ejecutar el siguiente comando:
```bash
sudo docker build -t dario248/airflow:slim_2_6_2 .
```
Es importante mantener el nombre de la imagen para que el archivo `docker-compose.yml` cree correctamente el contenedor

### Generar archivo .env
- Crear el archivo de variables de entorno con las siguientes variables:
```bash
REDSHIFT_HOST=***
REDSHIFT_PORT=5439
REDSHIFT_DB=***
REDSHIFT_USER=***
REDSHIFT_SCHEMA=***
REDSHIFT_PASSWORD=***
REDSHIFT_URL="jdbc:postgresql://${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}?user=${REDSHIFT_USER}&password=${REDSHIFT_PASSWORD}"
DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar
```

### Montar container de Docker
- Posicionarse en el root del repositorio
- Ejecutar
```bash
sudo docker-compose up --build
```
### Configuración Airflow Server
- Una vez levantado el servicio, dirigirse a `http://localhost:8080/`
- Ingresar con las credenciales de Airflow
  - user: airflow
  - password: airflow
- Configurar la conexión a la base de datos de Redshift en `Admin/Connections`
  - **Conn Id**: redshift_default
  - **Conn Type**: Amazon Redshift
  - **Host**: host de redshift
  - **Database**: base de datos de redshift
  - **Schema**: esquema de redshift
  - **User**: usuario de redshift
  - **Password**: contraseña de redshift
  - **Port**: 5439
- Configurar variables de Airflow
  - `DOLAR_FLAG_THRESHOLD`: 1
  - `dolar_variation_limit`: 0.01
  - `python_scripts_dir`: /opt/airflow/scripts
  - `SMTP_EMAIL_FROM`: cuenta de email del remitente de alertas
  - `SMTP_EMAIL_TO`: cuenta de email a donde enviar las alertas
  - `SMTP_PASSWORD`: token de acceso para enviar mails
  ![variables_airflow](https://github.com/dario248/EntregaFinal_DarioPaez_DATENG_51935/assets/78042437/c08192b4-5349-4986-a3f1-9c10ac972f4a)

### Ejecutar DAG etl_entrega_final
Una vez realizadas las configuraciones de conexion y variables ejectuar el DAG. 

## Estructura del DAG
El DAG que orquesta las tareas se encuentra definida en el script `dags/etl_entrega_final.py`. Consiste de una secuencia de tareas cuyo orden de prioridad se explica a continuación:

1. **PythonOperator** -> obtiene la variable `process_date` como el tiempo actual y lo envía a al entorno de Airflow para que esté disponible para ser accedida por otras tareas.
2. **SQLExecuteQueryOperator** -> ejecuta una query de SQL para crear la tabla donde se guardaran los datos obtenidos de la API tipo de cambio.
3. **SQLExecuteQueryOperator** -> ejecuta una query de SQL para crear la tabla donde se guardaran los datos obtenidos de la API de indice de precios al consumidor (IPC)
4. **SQLExecuteQueryOperator** -> ejecuta una query de SQL para crear la tabla donde se guardara un analisis comparativo del tipo de cambio y el IPC.
5. **SQLExecuteQueryOperator** -> ejecuta una query para limpiar registros que tengan el mismo process_date que al momento de ejecución.
6. **BashOperator** -> ejecuta el script de python ``scripts/etl_pandas.py`` pasándole como argumento el valor de `dolar_variation_limit`
7. **PythonOperator** -> ejecuta la función `check_dolar_values` que comprueba si la condición de variación del precio del dólar se cumple para enviar un email de alerta.

## Proceso ETL

- Se extraen datos de la **API pública del gobierno de Argentina**. Se extraen datos de dos tablas diferentes:
  - Serie de tiempo de **tipo de cambio dólar vendedor** del Banco Nación en pesos, con frecuencia diaria. [LINK](https://datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26&start_date=2018-07&limit=5000)
  - Serie de tiempo de **Indice de Precios al Consumidor Nacional (IPC)**, que se mide de forma mensual. [LINK](https://datos.gob.ar/series/api/series/?ids=148.3_INUCLEONAL_DICI_M_19) 
- Extracción de datos de las dos APIs mencionadas y se instancian los DataFrames de Pandas con los datos obtenidos
- Para poder combinar los datos en la tabla de analisis, es necesario convertir la columna `date` para que sea del tipo `datetime.date`. 
- Una vez ejecutado eso, se realiza una agregación de la tabla `tipo_de_cambio` de forma mensual, obteniendo el promedio del tipo de cambio vendedor.
- Luego, se realiza un merge entre ambas tablas obteniendo la tabla `df_analisis`
- Las tres tablas son cargadas en la base de datos de Redshift después de agregarles una columna con la variable `process_date`

## Alertas
Se establecen dos tipos de alertas distintas dentro del DAG para enviar emails dando avisos.

### Alerta de ejecución
Se establece una alerta que nos indicará en un email si la ejecución del DAG fue exitosa o hubo algún fallo, indicando el id de ejecución.
![dag_failed](https://github.com/dario248/EntregaFinal_DarioPaez_DATENG_51935/assets/78042437/a6379fe3-66f1-495d-a89b-5abf32cb5f07)
![dag_success](https://github.com/dario248/EntregaFinal_DarioPaez_DATENG_51935/assets/78042437/e78f99b9-97a4-4614-88ba-0ae7edae2faf)

### Alerta de aumento del dólar
Un gurú de las inversiones nos recomienda analizar las variaciones del dólar en los últimos 20 días. Si estas variaciones superan el 2% en 4 o más días, nos asegura que es muy probable que el dólar tenga un incremento notable del precio. 

Por lo tanto, definimos un análisis de las variaciones del dólar teniendo en cuenta un determinado umbral para recibir un alerta si se viene un gran incremento en el precio del dólar.

Obteniendo datos hasta el 30 de junio de 2023 se recomienda utilizar un valor de `dolar_variation_limit`= 0.01 y `DOLAR_FLAG_THRESHOLD`= 1 para comprobar que la alerta funciona correctamente. 
![image](https://github.com/dario248/EntregaFinal_DarioPaez_DATENG_51935/assets/78042437/7b619fda-d7a5-42f2-a4d8-465dc401d893)

## Resultados
**Tablas cargadas en Redshift**
![carga_tipo_cambio](https://github.com/dario248/EntregaFinal_DarioPaez_DATENG_51935/assets/78042437/221793e1-efb2-4197-b420-af7ff5d44e07)
![carga_ipc](https://github.com/dario248/EntregaFinal_DarioPaez_DATENG_51935/assets/78042437/6664d313-19ad-4699-9cc1-70e5b0d06908)
![carga_analisis](https://github.com/dario248/EntregaFinal_DarioPaez_DATENG_51935/assets/78042437/63531928-86c7-4a9f-9a4c-d1df7d4e32ce)


**Ejecución correcta del DAG**

![correcta_ejecucion_etl_2](https://github.com/dario248/EntregaFinal_DarioPaez_DATENG_51935/assets/78042437/6e4c3adb-2bee-4974-a315-fb8dd0250308)
