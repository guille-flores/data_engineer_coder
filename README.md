# Data Engineering

## Coderhouse 
Comisión 61890

## Prerequisitos
Tener un archivo dentro de la carpeta "/config/" llamado ".env" donde se tenga la siguiente información nombrada tal y como se especifica:

| Name | Description | Example |
| --- | --- | --- |
| AWS_REDSHIFT_USERNAME | User Name to access Redshift | "user_name?example" |
| AWS_REDSHIFT_PASSWORD |  Password to access Redshift | "casdsafa34234psswrd" |
| AWS_REDSHIFT_HOST | The Redshift host as "cluster_name.cluster_id.region.redshift.amazonaws.com" | "cluster_name.cluster_id.region.redshift.amazonaws.com" |
| AWS_REDSHIFT_PORT | The port to connect to the host | "3333" |
| AWS_REDSHIFT_DB | The Redshift database to connect with | "data-engineer-database" |
| AWS_REDSHIFT_SCHEMA | The Redshift [schema](https://docs.aws.amazon.com/redshift/latest/dg/r_Schemas_and_tables.html) to connect with | "db_schema" |
| POLYGON_BEARER_TOKEN | The Bearer Token of the Polygon API account to obtain financial data - [Polygon API Docs](https://polygon.io/docs/stocks/getting-started) | "asdfdfdfdfsdnAFDSFEewrWER" |
| GOOGLE_APP_PASSWORD | Your Google Account new (or already existing) Application Password to send email alerts related to US Stocks that are outside the designated thresholds definidos en el archivo /dags/utils/config_alert.json - [Google App Passwords](https://support.google.com/accounts/answer/185833?hl=en) | "abcd efgh ijkl mnop" |

## Estructura del Proyecto/Carpetas

La estructura del proyecto es de la siguiente manera:

<ul>
    <li><strong>proyecto/</strong>: Carpeta raíz de tu proyecto.</li>
    <li><strong>config/</strong>: Contiene archivos de configuración.
        <ul>
            <li><strong>.env.example</strong>: Archivo de ejemplo para las variables de entorno. Debe ser renombrado como ".env" y modificar las variables con el valor correcto en lugar de dejarlas como texto vacío "".</li>
        </ul>
    </li>
    <li><strong>dags/</strong>: Carpeta principal para los archivos de Airflow DAGs.
        <ul>
            <li><strong>etl.py</strong>: Archivo de script para las tareas de ETL.</li>
            <li><strong>main_dags.py</strong>: Archivo principal que define los DAGs.</li>
            <li><strong>utils/</strong>: Subcarpeta dentro de <code>dags/</code> para funciones auxiliares.
                <ul>
                    <li><strong>etl_functions.py</strong>: Archivo con funciones auxiliares para ETL (funciones que se usan en el archivo etl.py).</li>
                    <li><strong>config_alert.json</strong>: Archivo JSON conteniendo la configuración de alerta via email. Solicita el email de envío, el email destino, así como un conjunto de acciones y el treshold para cada una de ellas (puede ser modificado a gusto del usuario para recibir alertas de su interés).</li>
                </ul>
            </li>
        </ul>
    </li>
    <li><strong>logs/</strong>: Carpeta para almacenar los archivos de registro generados. Esta carpeta se llenará automáticamente al correr el programa, almacenando los registros de Airflow.</li>
    <li><strong>plugins/</strong>: Carpeta para almacenar plugins personalizados para Airflow. De igual forma, se llenará automáticamente de ser necesario.</li>
    <li><strong>requirements.txt</strong>: Archivo de texto con las dependencias de Python. Estas dependencias se instalarán automáticamente al correr el programa.</li>
    <li><strong>docker-compose.yml</strong>: Archivo de configuración para Docker Compose.</li>
    <li><strong>Dockerfile</strong>: El Dockerfile necesario para crear el contenedor. Este usa la imagen de Apache Airflow <a href="https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml">docker-compose.yaml</a> además de instalar las dependencias necesarias del archivo "requirements.txt".</li>
    <li><strong>.env.example</strong>: Las variables de entorno de Airflow en específico, renombrar por ".env" y <strong>no es necesario modificar</strong> ya que usa una imagen que ya se creó con el Dockerfile. Solo modificar la imagen por el nombre que se cree al correr el comando "docker build -t 'image name'" en caso de querer usar otra imagen. Este archivo solo tiene el User ID de Airflow (AIRFLOW_UID) y el archivo donde se encuentran los requisitos de instalaciones adicionales (AIRFLOW_IMAGE_NAME).</li>
</ul>

```
proyecto/
│
├── config/
│   └── .env.example (renombrar por .env y agregar los valores correspondientes)
│
├── dags/
│   ├── etl.py (código donde se tiene el paso a paso de ETL)
│   ├── main_dags.py (el código donde se programa el DAG de airflow y se llama a etl.py)
│   └── utils/
│       └── etl_functions.py (las funciones de ETL que usa el archivo etl.py)
│       └── config_alert.json (configuración de envío de alertas de stocks según un threshold definido y a quién se le envía la alerta)
│
├── logs/
│   └── (Aquí se almacenarán automáticamente los archivos de registro de airflow)
│
├── plugins/
│   └── (Aquí se almacenarán automáticamente los plugins personalizados de Airflow)
│
├── requirements.txt (las librerías necesarias de python para correr el proyecto)
└── docker-compose.yml (para crear y correr el contenedor en Airflow)
└── Dockerfile (Para obtener la imagen de apache Airflow e instalar los requirements.txt)
└── .env.example (variables de entorno de Airflow en específico, renombrar por ".env" y no modificarlo a menos que se quiera usar una nueva imagen creada por medio del Dockerfile.)
```

El archivo "docker-compose.yml" contiene todas las instrucciones para correr y programar el flujo diario de ingesta de datos, corriendo diariamente las instrucciones de "/dags/main_dags.py".

## Instrucciones

### Correr con la imagen de Dockerfile creada y publicada en Dockerhub 

[Imagen de Docker Hub - guilleflores/data-engineer-app](https://hub.docker.com/r/guilleflores/data-engineer-app/tags)

1. Renombrar el archivo ".env.example" por ".env". Este archivo se encuentra en la ruta padre del proyecto (no confundir con el archivo ".env" que se encuentra dentro de la carpeta "/config/"). <strong>NO modificar este archivo.</strong>. Este archivo define el ID del ususario de Airflow así como la imagen a usar de Docker Hub [guilleflores/data-engineer-app](https://hub.docker.com/r/guilleflores/data-engineer-app/tags).
2. Dentro de la carpeta "/config/" renombrar el archivo ".env.example" por ".env", y llenar con las variables necesarias que se mencionaron anteriormente en la sección de [Prerequisitos](#prerequisitos).
3. Dentro de la carpeta "/dags/utils/" buscar el archivo ".config_alert.json" y modificar el correo electrónico usado para enviar alertas y el correo que recibirá las alertas, así como la lista de acciones/stocks y su threshold correspondiente (precio min y max).
4. Antes de correr el programa, asegúrate de contar con las carpetas logs, plugins, dags y config. En caso de que no existan, puedes crearlas tú mismo de manera manual o con los siguientes comandos:
```
#located under the parent folder
> ~\project  mkdir -p /logs
> ~\project  mkdir -p /plugins
> ~\project  mkdir -p /config
```
5. Correr en la terminal el archivo "docker-compose.yml" estando en la misma carpeta que el mismo.
```
#located under the parent folder
> ~\project  docker compose up
```
6. Acceder al puerto de internet "http://127.0.0.1:8080/" para visualizar la UI de Airflow.
7. Acceder en la UI de Airflow el ususario y contraseña "airflow".


### Correr con una nueva imagen local creada a partir del Dockerfile 

1. Dentro de la carpeta "/config/" renombrar el archivo ".env.example" por ".env", y llenar con las variables necesarias que se mencionaron anteriormente en la sección de [Prerequisitos](#prerequisitos).
2. Antes de correr el programa, asegúrate de contar con las carpetas logs, plugins, dags y config. En caso de que no existan, puedes crearlas tú mismo de manera manual o con los siguientes comandos:
```
#located under the parent folder
> ~\project  mkdir -p /logs
> ~\project  mkdir -p /plugins
> ~\project  mkdir -p /config
```
3. Correr en la terminal el comando para construir la imagen con el tag/nombre deseado, por ejemplo, <b>data-engineer-app</b>.
```
#located under the parent folder
> ~\project  docker build -t data-engineer-app
```
4. Renombrar el archivo ".env.example" por ".env". Este archivo se encuentra en la ruta padre del proyecto (no confundir con el archivo ".env" que se encuentra dentro de la carpeta "/config/"). Este archivo define el ID del ususario de Airflow así como la imagen a usar. 
5. Dentro del archivo ".env" del paso anterior, modificar el valor que se tiene para "AIRFLOW_IMAGE_NAME" por el nombre asignado a la imagen construida en el paso anterior. Por ejemplo, AIRFLOW_IMAGE_NAME=data-engineer-app.
6. Dentro de la carpeta "/dags/utils/" buscar el archivo ".config_alert.json" y modificar el correo electrónico usado para enviar alertas y el correo que recibirá las alertas, así como la lista de acciones/stocks y su threshold correspondiente (precio min y max).
7. Correr en la terminal el archivo "docker-compose.yml" estando en la misma carpeta que el mismo.
```
#located under the parent folder
> ~\project  docker compose up
```
8. Acceder al puerto de internet "http://127.0.0.1:8080/" para visualizar la UI de Airflow.
9. Acceder en la UI de Airflow el ususario y contraseña "airflow".


## Detalle del proceso ETL de Python realizado

Lo que realiza el en la ingesta con Python es lo siguiente:
1. Primero instala las dependencias necesarias que se encuentran en el archivo "requirements.txt", asegurando que se tengan los modulos necesarios para correr el código.
2. El archivo "requirements.txt" tiene entre sus modulos el uso de pandas, de psycopg2, requests y dotenv.
3. Posteriormente, dispara la función main(), la cual viene del archivo llamado etl.py
4. El archivo etl.py consta de una función, main().
5. La función main, a su vez, hace uso de las funciones que están dentro del archivo "etl_functions.py" dentro de la carpeta "/dags/utils/".
6. Las acciones que se realizan en el archivo main_code son las siguientes:
    - Se obtiene información de acciones del día anterior por medio de Polygon API y el endpoint /v2/aggs/grouped/locale/us/market/stocks/{date} de [Grouped Daily (Bars)](https://polygon.io/docs/stocks/get_v2_aggs_grouped_locale_us_market_stocks__date)
    - Se escriben los valored de JSON a una Dataframe (DF)
    - Se transforman y manipulan los valores de la DF: se renombran columnas para que sea más entendible, se cambian tipos de datos, se agregan columnas del tiempo actual en que se corre el script.
    - Posteriormente, se conecta con Redshift para crear una tabla e insertar los datos de la DF relacionados a las acciones del día de ayer. 
    - Por último, hacer una query a Redshift para solicitar 10 registros para visualizar y validar.