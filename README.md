# Athena Data Flow Script

💡 **Nota**: Amazon Athena es un servicio de consulta interactivo que facilita el análisis de datos en Amazon S3 utilizando SQL estándar. Para más detalles sobre cómo utilizar Athena, puedes consultar la [guía para usar Amazon Athena](https://www.notion.so/d4caea8159254ad0b55b60c635e0b6b0?pvs=21).

## Estructura del Proyecto

- **src/**: Contiene los scripts de Python para el procesamiento de datos.
  - **base/**: Contiene los esquemas de base de datos utilizados en el proyecto.
  - **flow_*.py**: Scripts que implementan la lógica específica para cada tipo de procesamiento de datos (`pets`, `prod`, y datos OTU).
- **run.py**: Script principal que orquesta la ejecución basada en los parámetros proporcionados.


## Uso del Script

### Argumentos del Script

El script acepta los siguientes argumentos para personalizar el procesamiento de datos:

- `--project_id`: El ID del proyecto para el que se procesarán los datos.
- `--type`: Tipo de datos a procesar (`pets` o `prod`).
- `--otu`: Un parámetro opcional que indica que el script debe procesar datos OTU.

### Implementación

#### Funciones Principales

- `load_pet_table_athena`: Procesa los datos para proyectos de tipo 'pets', cargando datos en DataFrame, filtrando por ID de proyecto y almacenando los resultados en S3 y Athena.
- `load_prod_table_athena`: Similar a la función anterior pero para proyectos de tipo 'prod'.
- `load_prod_otucount_table_athena`: Procesa datos OTU para proyectos 'prod', manejando la carga de datos, su procesamiento, y la integración con S3 y Athena.
- `load_pet_otucount_table_athena`: Función análoga a la anterior, pero adaptada para proyectos 'pets'.

### Flujo de Trabajo

1. **Consulta de Datos**: El script inicia extrayendo los datos necesarios desde el data warehouse en Redshift.
2. **Creación de Archivos Parquet**: Crea archivos Parquet para cada proyecto, almacenándolos en `s3://siwaathena/projects/full-data/<nombre_del_proyecto>`.
3. **Uso de Datos**: Los archivos están disponibles para ser consultados y utilizados con cualquier herramienta compatible con el formato Parquet.
4. **Integración con Athena**: Se configuran y actualizan tablas en Athena bajo la base de datos `siwa_adb`, en el grupo de trabajo `siwa-data`, facilitando las consultas SQL directas sobre los datos procesados.


### Ejemplo de Comando

Ejecuta el script en la terminal con el siguiente comando:

```bash
python run.py --project_id=ExampleID --type=pets
```

Para procesar datos OTU para un proyecto específico:

```bash
python run.py --project_id=ExampleID --type=prod --otu
```
