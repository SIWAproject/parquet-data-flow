import io
import os
import boto3
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
from src.utils import check_query_status, export_geneexpression_to_csv, fetch_animals_pets_to_dataframe, fetch_animals_prod_to_dataframe, fetch_kits_prod_to_dataframe, fetch_microbiome_to_dataframe, export_histopathology_to_dataframe
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
import logging


logging.basicConfig()
logging.getLogger('sqlalchemy').setLevel(logging.WARNING)

def load_prod_table_athena(project_ids):

    project_ids = project_ids
    project_name = project_ids[0]
    s3_client = boto3.client('s3')
    bucket_name = 'siwaathena'
    database_name = "siwa_adb"  
    athena_client = boto3.client('athena', region_name='us-east-2')


    # Configurar logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)


    # Llamar a la función para obtener el DataFrame
    df_kits = fetch_kits_prod_to_dataframe()
    df_animals = fetch_animals_prod_to_dataframe()
    df_pets = fetch_animals_pets_to_dataframe()
    df_microbiome = fetch_microbiome_to_dataframe()
    df_histo = export_histopathology_to_dataframe()

    # Agrupar los datos por 'Sample ID' y asegurarse de que los valores de las demás columnas sean únicos
    df_histo_grouped = df_histo.groupby('Sample ID').agg({
        'Animal ID': 'first',
        'Sample Location': 'first',
        'Research Number': 'first',
        'Unique Key': 'first',
        'Score': list,
        'Value': list
    }).reset_index()

    rows = []
    for _, row in df_histo_grouped.iterrows():
        for score, value in zip(row['Score'], row['Value']):
            rows.append({
                'Sample ID': row['Sample ID'],
                'Animal ID': row['Animal ID'],
                'Sample Location': row['Sample Location'],
                'Research Number': row['Research Number'],
                'Unique Key': row['Unique Key'],
                'Score': score,
                'Value': value
            })

    df_expanded = pd.DataFrame(rows)

    # Crear el DataFrame pivotado
    df_pivoted_histo = df_expanded.pivot_table(index=['Sample ID', 'Animal ID', 'Sample Location', 'Research Number', 'Unique Key'], 
                                        columns='Score', 
                                        values='Value', 
                                        aggfunc='first')

    df_pivoted_histo.reset_index(inplace=True)


    df_gen = export_geneexpression_to_csv()

    kit_ids = df_kits[df_kits['Project ID'].isin(project_ids)]['Kit ID'].unique()

    animal_ids = df_animals[df_animals['Kit ID'].isin(kit_ids)]['Animal ID'].unique()

    df_microbiome_filtered = df_microbiome[df_microbiome['Animal ID'].isin(animal_ids)]

    df_gen_filtered = df_gen[df_gen['Animal ID'].isin(animal_ids)]

    df_pivoted_histo = df_pivoted_histo[df_pivoted_histo['Animal ID'].isin(animal_ids)]

    df_microbiome_filtered['Key'] = df_microbiome_filtered['Sample ID'] + '-' + df_microbiome_filtered['Sample Location']

    df_gen_filtered['Key'] = df_gen_filtered['Sample ID'] + '-' + df_gen_filtered['Sample Location']

    full_data = pd.merge(df_microbiome_filtered, df_gen_filtered, on='Key', how='inner')

    df_microbiome_filtered = df_microbiome[df_microbiome['Animal ID'].isin(animal_ids)]

    df_gen_grouped = df_gen.groupby('Sample ID').agg({
        'Plate Code': 'first',  
        'Animal ID': 'first',
        'Sample Location': 'first',
        'Target Gene': list,
        'Delta Cq': list
    }).reset_index()

    # Expandir las listas en filas separadas
    rows = []
    for _, row in df_gen_grouped.iterrows():
        for gene, delta_cq in zip(row['Target Gene'], row['Delta Cq']):
            rows.append({
                'Sample ID': row['Sample ID'],
                'Plate Code': row['Plate Code'],
                'Animal ID': row['Animal ID'],
                'Sample Location': row['Sample Location'],
                'Target Gene': gene,
                'Delta Cq': delta_cq
            })

    df_expanded = pd.DataFrame(rows)

    pivoted_df = df_expanded.pivot_table(index=['Sample ID', 'Plate Code', 'Animal ID', 'Sample Location'], 
                                        columns='Target Gene', 
                                        values='Delta Cq', 
                                        aggfunc='first')

    pivoted_df.reset_index(inplace=True)

    df_gen_filtered = pivoted_df[pivoted_df['Animal ID'].isin(animal_ids)]

    df_histo_filtered = df_pivoted_histo[df_pivoted_histo['Animal ID'].isin(animal_ids)]

    df_histo_filtered['Sample ID'] = df_histo_filtered['Sample ID'].str.replace('-H$', '', regex=True)

    df_gen_filtered['Sample ID'] = df_gen_filtered['Sample ID'].str.replace('-G$', '', regex=True)

    df_microbiome_filtered['Sample ID'] = df_microbiome_filtered['Sample ID'].str.replace('-M$', '', regex=True)

    merged_df = pd.merge(df_microbiome_filtered, df_gen_filtered, on='Sample ID', how='left')

    merged_df = pd.merge(merged_df, df_histo_filtered, on='Sample ID', how='left')

    merged_df['Animal ID'] = merged_df['Animal ID_x'].combine_first(merged_df['Animal ID_y'])

    merged_df['Sample Location'] = merged_df['Sample Location_x'].combine_first(merged_df['Sample Location_y'])

    merged_df.drop(columns=['Animal ID_x', 'Animal ID_y', 'Sample Location_x', 'Sample Location_y'], inplace=True)

    filtered_animals = df_animals[df_animals['Kit ID'].isin(kit_ids)]

    sorted_animals = filtered_animals.sort_values(by='Kit ID')

    merged_df_animal = pd.merge(df_animals, df_kits, on='Kit ID', how='left')

    final_merged_df = pd.merge(merged_df_animal, merged_df, on='Animal ID', how='inner')

    parquet_buffer = io.BytesIO()

    table = pa.Table.from_pandas(final_merged_df)

    pq.write_table(table, parquet_buffer)

    file_name = f"projects/full-data/{project_name}/{project_name}.parquet"

    # Subir el archivo Parquet a S3
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=parquet_buffer.getvalue())

    column_defs = []
    for column, dtype in final_merged_df.dtypes.items():
        if dtype == 'object':
            column_defs.append(f"`{column}` string")
        elif dtype in ['int64', 'float64']:
            column_defs.append(f"`{column}` double")
        else:
            raise ValueError(f"Tipo de dato no soportado para la columna {column}: {dtype}")

    table_schema = ",\n    ".join(column_defs)


    # Ahora, crear la tabla en Athena
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


