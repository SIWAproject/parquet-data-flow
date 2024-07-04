import boto3
import io
import os
import time
import pandas as pd
import sqlalchemy as sa 
from sqlalchemy import  MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
import logging
from io import BytesIO
from pathlib import Path
import logging
import os
from sqlalchemy.engine.url import URL
import sqlalchemy as sa
from io import StringIO
import logging
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
import psycopg2
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Float, Integer, String
from src.utils import check_query_status, export_geneexpression_to_csv, fetch_animals_pets_to_dataframe, fetch_animals_prod_to_dataframe, fetch_kits_prod_to_dataframe, fetch_microbiome_to_dataframe, export_histopathology_to_dataframe, fetch_pets_kits_to_dataframe
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
import logging


logging.basicConfig()
logging.getLogger('sqlalchemy').setLevel(logging.WARNING)

def load_pet_table_athena(project_ids):
    import boto3
    project_ids = project_ids
    project_name = project_ids[0]
    s3_client = boto3.client('s3')
    bucket_name = 'siwaathena'
    database_name = "siwa_adb"  
    athena_client = boto3.client('athena', region_name='us-east-2')


    # Configurar logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)


    df_pets_kits = fetch_pets_kits_to_dataframe()
    df_pets = fetch_animals_pets_to_dataframe()
    df_microbiome = fetch_microbiome_to_dataframe()
    kit_ids = df_pets_kits[df_pets_kits['Project ID'].isin(project_ids)]['Kit ID'].unique()
    animal_ids = df_pets[df_pets['Kit ID'].isin(kit_ids)]['Animal ID'].unique()
    df_microbiome_filtered = df_microbiome[df_microbiome['Animal ID'].isin(animal_ids)]
    df_microbiome_filtered['Sample ID'] = df_microbiome_filtered['Sample ID'].str.replace('-M$', '', regex=True)
    filtered_animals = df_pets[df_pets['Kit ID'].isin(kit_ids)]
    # Ordenar df_animals por 'Kit ID' para agrupar las filas por este campo
    sorted_animals = filtered_animals.sort_values(by='Kit ID')
    merged_df_animal = pd.merge(df_pets, df_pets_kits, on='Kit ID', how='left')
    df_microbiome_filtered['Animal Sample ID'] = df_microbiome_filtered['Sample ID'].str.rstrip('F')


    final_merged_df = pd.merge(
        merged_df_animal,           # DataFrame izquierdo
        df_microbiome_filtered,     # DataFrame derecho
        left_on='Animal Sample ID', # Columna del DataFrame izquierdo para hacer el merge
        right_on='Animal Sample ID',       # Columna del DataFrame derecho para hacer el merge
        how='inner'                 # Tipo de merge, 'inner' significa la intersección de las claves
    )



    # Combinar 'Animal ID_x' con 'Animal ID_y'
    final_merged_df['Animal ID'] = final_merged_df['Animal ID_x'].combine_first(final_merged_df['Animal ID_y'])

    # final_merged_df['Sample ID'] = final_merged_df['Animal Sample ID'].combine_first(final_merged_df['Sample ID'])
    final_merged_df.drop(columns=['Animal ID_x', 'Animal ID_y', 'Animal Sample ID'], inplace=True)

    parquet_buffer = io.BytesIO()
    table = pa.Table.from_pandas(final_merged_df)
    pq.write_table(table, parquet_buffer)

    # Obtener el nombre del archivo usando el nombre del proyecto
    file_name = f"projects/full-data/{project_name}/{project_name}.parquet"

    # Crear el cliente de S3
    s3_client = boto3.client('s3')

    # Especificar el nombre del bucket
    bucket_name = 'siwaathena'

    # Subir el archivo Parquet a S3
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=parquet_buffer.getvalue())

    print(f"Archivo Parquet subido exitosamente a: s3://{bucket_name}/{file_name}")


    column_defs = []
    for column, dtype in final_merged_df.dtypes.items():
        if dtype == 'object':
            column_defs.append(f"`{column}` string")
        elif dtype in [ 'float64']:
            column_defs.append(f"`{column}` double")
        elif dtype in [ 'int64']:
            column_defs.append(f"`{column}` int")
        else:
            raise ValueError(f"Tipo de dato no soportado para la columna {column}: {dtype}")

    table_schema = ",\n    ".join(column_defs)
    table_schema
    import boto3
    import time
    # Ahora, crear la tabla en Athena
    database_name = "siwa_adb"  # Reemplaza con el nombre de tu base de datos en Athena
    s3_data =  f"s3://{bucket_name}/projects/full-data/{project_name}/"# Ubicación del archivo Parquet en S3
    s3_output_location = f"s3://{bucket_name}/output/"  # Ubicación para los resultados de la consulta

    # Crear la consulta de creación de tabla
    create_table_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{project_name}_full_data (
        {table_schema}
    )
    STORED AS PARQUET
    LOCATION '{s3_data}'
    TBLPROPERTIES ('classification'='parquet');
    """

    # Crear el cliente de Athena
    athena_client = boto3.client('athena', region_name='us-east-2')

    # Ejecutar la consulta en Athena
    response = athena_client.start_query_execution(
        QueryString=create_table_query,
        QueryExecutionContext={
            'Database': database_name
        },
        ResultConfiguration={
            'OutputLocation': s3_output_location  # Ubicación para guardar los resultados de la consulta
        }
    )

    # Obtener el ID de la ejecución de la consulta
    query_execution_id = response['QueryExecutionId']

    # Función para verificar el estado de la consulta
    def check_query_status(query_execution_id):
        result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = result['QueryExecution']['Status']['State']
        return status, result

    # Esperar hasta que la consulta se complete
    status, result = check_query_status(query_execution_id)
    while status in ['QUEUED', 'RUNNING']:
        print(f"Estado actual de la consulta: {status}")
        time.sleep(5)
        status, result = check_query_status(query_execution_id)

    # Verificar si la consulta falló y obtener el mensaje de error
    if status == 'FAILED':
        error_message = result['QueryExecution']['Status']['StateChangeReason']
        print(f"Error en la consulta: {error_message}")
    elif status == 'SUCCEEDED':
        print("Consulta ejecutada exitosamente.")
    else:
        print(f"Estado final de la consulta: {status}")
    pass

