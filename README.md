# Data Engineering

## Coderhouse 
Comisión 61890

## Prerequisitos
Tener un archivo dentro de la carpeta "/dags/config/" llamado ".env" donde se tenga la siguiente información nombrada tal y como se especifica:

| Name | Description | Example |
| --- | --- | --- |
| AWS_REDSHIFT_USERNAME | User Name to access Redshift | "user_name?example" |
| AWS_REDSHIFT_PASSWORD |  Password to access Redshift | "casdsafa34234psswrd" |
| AWS_REDSHIFT_HOST | The Redshift host as "cluster_name.cluster_id.region.redshift.amazonaws.com" | "cluster_name.cluster_id.region.redshift.amazonaws.com" |
| AWS_REDSHIFT_PORT | The port to connect to the host | "3333" |
| AWS_REDSHIFT_DB | The Redshift database to connect with | "data-engineer-database" |
| AWS_REDSHIFT_SCHEMA | The Redshift [schema](https://docs.aws.amazon.com/redshift/latest/dg/r_Schemas_and_tables.html) to connect with | "db_schema" |
| POLYGON_BEARER_TOKEN | The Bearer Token of the Polygon API account to obtain financial data - [Polygon API Docs](https://polygon.io/docs/stocks/getting-started) | "asdfdfdfdfsdnAFDSFEewrWER" |

## Instrucciones

1. Dentro de la carpeta "/dags/config/" renombrar el archivo ".env.example" por ".env", y llenar con las variables necesarias que se mencionaron anteriormente en los prerrequisitos.
2. El archivo "docker-compose.yml" contiene todas las instrucciones para correr y programar el flujo diario de ingesta de datos, corriendo diariamente las instrucciones de "/dags/src/main.py".
3. Antes de correr el comando, asegúrate de contar con las carpetas logs, plugins, dags y config.
4. Correr en la terminal el archivo "docker-compose.yml" estando en la misma carpeta que el mismo.

```
#located under the parent folder
> ~\project  docker compose up
```

Lo que realiza el en la ingesta con Python es lo siguiente:
1. Primero instala las dependencias necesarias que se encuentran en el archivo "requirements.txt", asegurando que se tengan los modulos necesarios para correr el código.
2. El archivo "requirements.txt" tiene entre sus modulos el uso de pandas, de psycopg2, requests y dotenv.
3. Posteriormente, dispara la función main(), la cual viene del archivo llamado etl.py
4. El archivo etl.py consta de una función, main().
5. La función main, a su vez, hace uso de las funciones que están dentro del archivo "etl_functions.py" dentro de la carpeta "/dags/src/utils/".
6. Las acciones que se realizan en el archivo main_code son las siguientes:
    - Se obtiene información de acciones del día anterior por medio de Polygon API y el endpoint /v2/aggs/grouped/locale/us/market/stocks/{date} de [Grouped Daily (Bars)](https://polygon.io/docs/stocks/get_v2_aggs_grouped_locale_us_market_stocks__date)
    - Se escriben los valored de JSON a una Dataframe (DF)
    - Se transforman y manipulan los valores de la DF: se renombran columnas para que sea más entendible, se cambian tipos de datos, se agregan columnas del tiempo actual en que se corre el script.
    - Posteriormente, se conecta con Redshift para crear una tabla e insertar los datos de la DF relacionados a las acciones del día de ayer. 
    - Por último, hacer una query a Redshift para solicitar 10 registros para visualizar y validar.