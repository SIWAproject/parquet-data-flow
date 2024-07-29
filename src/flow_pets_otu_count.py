import io
import boto3
import time
import pandas as pd
import logging
from src.utils import fetch_animals_pets_to_dataframe, fetch_pets_kits_to_dataframe, fetch_taxonomy_to_dataframe, get_pycon
import pyarrow as pa
import pyarrow.parquet as pq



logging.basicConfig()
logging.getLogger('sqlalchemy').setLevel(logging.WARNING)

def load_pet_otucount_table_athena(project_ids):

    project_ids = project_ids
    project_name = project_ids[0]
    s3_client = boto3.client('s3')
    bucket_name = 'siwaathena'
    database_name = "siwa_adb"  
    athena_client = boto3.client('athena', region_name='us-east-2')

    # Definir el tamaño de cada lote para la carga de datos
    chunk_size = 10000
    chunks = []

    # Usa la función get_pycon para obtener una conexión psycopg2 directa
    with get_pycon() as connection:
        for chunk in pd.read_sql('SELECT * FROM public.otucount', con=connection, chunksize=chunk_size):
            chunks.append(chunk)

    # Concatenar todos los lotes en un único DataFrame
    df_otu_counts = pd.concat(chunks, ignore_index=True)

    # Renombrar la columna 'otu' a 'OTU' directamente en el DataFrame original
    df_otu_counts.rename(columns={'otu': 'OTU'}, inplace=True)

    df_taxonomy = fetch_taxonomy_to_dataframe()  # Recuperar datos en DataFrame

    # Realizar un merge (inner join) en la columna 'otu'
    merged_otutax_df = pd.merge(df_otu_counts, df_taxonomy, on='OTU', how='inner')

    # Llamar a la función para obtener el DataFrame
    df_kits = fetch_pets_kits_to_dataframe()
  
    df_animals = fetch_animals_pets_to_dataframe()

    kit_ids = df_kits[df_kits['Project ID'].isin(project_ids)]['Kit ID'].unique()

    animal_ids = df_animals[df_animals['Kit ID'].isin(kit_ids)]['Animal ID'].unique()

    filtered_df = merged_otutax_df[merged_otutax_df['sampleid'].str.match(r'.*[A-Z]-[A-Z]$')]
    
    filtered_df['Adjusted Sample ID'] = filtered_df['sampleid'].str.slice(0, -6)
    filtered_df = filtered_df[filtered_df['Adjusted Sample ID'].isin(animal_ids)]
    filtered_df['sampleid'] = filtered_df['sampleid'].str.slice(0, -2)

    pivot_df = filtered_df.pivot_table(index='OTU', columns='sampleid', values='value')

    merged_pivottax_df = pd.merge(pivot_df, df_taxonomy, on='OTU', how='inner')
    merged_pivottax_df.fillna(0, inplace=True) 
    parquet_buffer = io.BytesIO()
    table = pa.Table.from_pandas(merged_pivottax_df)
    pq.write_table(table, parquet_buffer)

    # Obtener el nombre del archivo usando el nombre del proyecto
    file_name = f"projects/otu-data/{project_name}/{project_name}.parquet"

    # Crear el cliente de S3
    s3_client = boto3.client('s3')

    # Especificar el nombre del bucket
    bucket_name = 'siwaathena'

    # Subir el archivo Parquet a S3
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=parquet_buffer.getvalue())

    print(f"Archivo Parquet subido exitosamente a: s3://{bucket_name}/{file_name}")

    column_defs = []
    for column, dtype in merged_pivottax_df.dtypes.items():
        if dtype == 'object':
            column_defs.append(f"`{column}` string")
        elif dtype in ['int64', 'float64']:
            column_defs.append(f"`{column}` double")
        else:
            raise ValueError(f"Tipo de dato no soportado para la columna {column}: {dtype}")

    table_schema = ",\n    ".join(column_defs)
    table_schema

    # Ahora, crear la tabla en Athena
    database_name = "siwa_adb"  # Reemplaza con el nombre de tu base de datos en Athena
    s3_data =  f"s3://{bucket_name}/projects/otu-data/{project_name}/"# Ubicación del archivo Parquet en S3
    s3_output_location = f"s3://{bucket_name}/output/"  # Ubicación para los resultados de la consulta


    # Crear la consulta de creación de tabla
    create_table_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{project_name}_otu_count (
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





