
# Data Flow Parquet Project

Este proyecto incluye scripts para consultar datos en redshift, procesarlos, convertirlos a formato Parquet y subirlos a AWS S3.

## Descripción

El proyecto automatiza la extracción de datos desde una base de datos SQL, su procesamiento y transformación en DataFrames de Pandas, la conversión de estos DataFrames a formato Parquet y, finalmente, su carga en un bucket de S3.

## Configuración del Entorno

Para ejecutar este proyecto, necesitarás Python 3.11.7 y algunas librerías adicionales.

### Instalación de Dependencias

Primero, instala las dependencias necesarias usando `pip`. Asegúrate de estar en el directorio raíz del proyecto y ejecuta:


```pip install -r requirements.txt```
Este comando instalará todas las librerías necesarias, como boto3, pandas, sqlalchemy, y otras dependencias.

Configuración de Variables de Entorno
Necesitarás configurar varias variables de entorno para conectar con tu base de datos y AWS S3. Crea un archivo .env en el directorio raíz del proyecto con el siguiente contenido:

```
DB_DRIVER=driver_aqui
DB_HOST=tu_host_aqui
DB_PORT=tu_puerto_aqui
DB_NAME=tu_nombre_de_bd_aqui
DB_USER=tu_usuario_aqui
DB_PASSWORD=tu_contraseña_aqui
AWS_ACCESS_KEY_ID=tu_access_key_id_aqui
AWS_SECRET_ACCESS_KEY=tu_secret_access_key_aqui
S3_REGION=tu_region_aqui
S3_BUCKET=tu_bucket_aqui
```

Ejecución
Para ejecutar el script principal, navega al directorio raíz del proyecto y ejecuta:

```
python main.py
```

Este comando iniciará el proceso de extracción, transformación y carga (ETL) definido en los scripts.

Estructura del Proyecto
El proyecto está organizado de la siguiente manera:

```
/raíz-del-proyecto
    /src
        /utils.py - Contiene las funciones de utilidad y lógica principal del ETL.
    /logs
        data-flow-parquet.log - Archivo de logs generado por el script.
    main.py - Punto de entrada para ejecutar el proceso ETL.
    .env - Archivo para configurar las variables de entorno (debe ser creado por el usuario).
    requirements.txt - Dependencias del proyecto
```

### Logs
Los logs de la ejecución se guardan en el directorio /logs bajo el nombre data-flow-parquet.log. 
