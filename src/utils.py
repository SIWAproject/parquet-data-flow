import io
import os
import boto3
import pandas as pd
import sqlalchemy as sa 
from sqlalchemy import  MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
from .base import FeatureCountExtendedView, SampleMetadataExtendedView
import logging
from io import BytesIO
from pathlib import Path
# %%

# Obtener la ruta del directorio actual (__file__ es la ruta del archivo actual)
current_directory = Path(__file__)

# Subir dos niveles para llegar a la raíz del proyecto
project_root = current_directory.parent.parent

# Definir la ruta de la carpeta de logs y el archivo de log
log_directory = project_root / 'logs'
log_file_path = log_directory / 'data-flow-parquet.log'

# Crear el directorio de logs si no existe
log_directory.mkdir(exist_ok=True)

# Configurar el logging para que use el archivo de log especificado
logging.basicConfig(filename=str(log_file_path), level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')
print(log_file_path )
# %%
# Cargar variables de entorno
load_dotenv()

# %%
# Configuración de la conexión a la base de datos
def get_engine():
    url = URL.create(
        drivername=os.getenv('DB_DRIVER'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        username=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    engine =sa.create_engine(url)
    return engine

engine = get_engine()
# Creación de MetaData y Declarative Base
metadata = MetaData()
Base = declarative_base(metadata=metadata)
# Creación de la sesión
Session = sessionmaker(bind=get_engine())
session = Session()

# %%
def client(client):
    return boto3.client(
    client,
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('S3_REGION')
    )
# Configurar el cliente de s3
s3_client = client('s3')
# Configurar el cliente de Athena
athena_client = client('athena')

# %%
def read_from_s3(bucket_name, object_name, s3_client):
    try:
        
        # Lee el archivo desde S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)

        # Lee los datos del contenido del archivo

        df = pd.read_csv(io.BytesIO(response['Body'].read()))

        return df

    except Exception as e:
        return {"error": str(e)}

# %%
def pivotear_datos(df):
    """
    Pivotea los datos de un DataFrame.

    Parámetros:
    df (DataFrame): DataFrame que contiene las columnas 'value', 'otu' y 'sampleId'.

    Devuelve:
    DataFrame: DataFrame pivoteado con 'otu' como índice y 'sampleId' como columnas.
    """

    # Crear una tabla pivote
    datos_pivoteados = (
        df.pivot_table(values='value', index='otu', columns='sampleId')
          .fillna(0)
          .rename_axis(index=None, columns=None)
    )

    # Restablecer el índice
    datos_pivoteados = datos_pivoteados.reset_index()

    return datos_pivoteados

# %%
def upload_project_parquet_to_s3(projectid, parquet, s3_client, bucket_name, dataType):
    """
    Sube un archivo Parquet a un bucket de S3 específico basado en el ID del proyecto.

    :param projectid: El ID del proyecto para el cual se subirá el archivo.
    :param otu_parquet: El contenido del archivo Parquet o la ruta al archivo a subir.
    :param s3_client: La instancia del cliente S3 para realizar la subida.
    """
    file_name = f"{dataType}/{projectid}/{projectid}.parquet"
    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=parquet)
        return {"message": "Archivo subido exitosamente"}
    except Exception as e:
        return {"error": str(e)}

# %%
def df_to_parquet(df):
 # df_otu_con_project es tu DataFrame
    parquet_buffer = df.to_parquet( engine='pyarrow', compression='snappy')
    return parquet_buffer

# %%
def get_projectid_all():
    """Consulta a la tabla FeatureCountExtendedView para obtener una lista de IDs de proyectos existentes."""
    # Ejecutar la consulta directamente y obtener los projectId como un dataframe
    df = pd.read_sql(session.query(FeatureCountExtendedView.projectId).statement, session.bind)
    
    # Filtrar los projectId nulos y obtener una lista única de projectId
    project_ids = df['projectId'].dropna().unique().tolist()
    
    return project_ids

# %%
    
def get_feature_otu(projectId):
    """Consulta a la tabla otu para obtener los datos de un project especifico"""
    query = session.query(
        FeatureCountExtendedView.value, 
        FeatureCountExtendedView.otu, 
        FeatureCountExtendedView.sampleId
    ).filter(FeatureCountExtendedView.projectId == projectId)
    df = pd.read_sql(query.statement, session.bind)
    
    return df.to_dict(orient='records')

# %%
def get_metadata_by_project_id(project_id=None):
    """Consulta a la tabla Microbiome con un límite especificado y opcionalmente filtra por projectId."""
        # Iniciar la consulta
    query = session.query(SampleMetadataExtendedView)

    # Filtrar por runId si se proporciona
    if project_id:
        query = query.filter(SampleMetadataExtendedView.projectId == project_id)

    # Aplicar el límite


    # Ejecutar la consulta y convertir el resultado en un diccionario
    df = pd.read_sql(query.statement, session.bind)
    return df.to_dict(orient='records')
    

# %%

def read_updated_projects(s3_client, s3_bucket, projects_parquet_key):
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=projects_parquet_key)
        buffer = BytesIO(response['Body'].read())
        # Use the buffer to read the Parquet file
        updated_projects_df = pd.read_parquet(buffer)
        updated_projects_set = set(updated_projects_df['project_id'])
        return updated_projects_set
    except Exception as e:
        logging.warning(f"No se pudo leer el archivo de proyectos actualizados desde S3: {e}")
        return set()

# %%
def process_otus_for_project(project_id, s3_client, s3_bucket):
    try:
        project_data = pd.DataFrame(get_feature_otu(project_id))
        if project_data.empty:
            logging.warning(f'Proyecto otu {project_id}: No se encontraron datos.')
            return

        otu_pivot = pivotear_datos(project_data)
        otu_parquet = df_to_parquet(otu_pivot)
        upload_project_parquet_to_s3(project_id, otu_parquet, s3_client, s3_bucket, 'otu')
        logging.info(f'Proyecto otu {project_id}: Datos subidos correctamente a S3.')
        return True
    except Exception as e:
        logging.error(f'Proyecto otu {project_id}: Error al procesar - {e}')
        return False

# %%

def update_projects_record(s3_client, s3_bucket, projects_parquet_key, updated_projects_set):
    updated_projects_df = pd.DataFrame(list(updated_projects_set), columns=['project_id'])
    updated_projects_parquet = df_to_parquet(updated_projects_df)
    s3_client.put_object(Bucket=s3_bucket, Key=projects_parquet_key, Body=updated_projects_parquet)

# %%

def upload_parquets_otus_for_projects(project_list, s3_client):
    s3_bucket = 'siwaparquets'
    projects_parquet_key = 'projects/projects_record/otu.parquet'
    
    updated_projects_set = read_updated_projects(s3_client, s3_bucket, projects_parquet_key)

    for project_id in project_list:
        if project_id in updated_projects_set:
            logging.info(f'Proyecto otus {project_id}: El archivo ya está en S3 y no se subirá nuevamente.')
            continue

        if process_otus_for_project(project_id, s3_client, s3_bucket):
            updated_projects_set.add(project_id)

    update_projects_record(s3_client, s3_bucket, projects_parquet_key, updated_projects_set)

    return "Proceso completado con éxito."

# %%
def merge_data_if_needed(project_data, extra_data):
    # Verificar si extra_data está vacío o es nulo antes de proceder
    if extra_data.empty:
        return project_data
    else:
        # Estandarizar los nombres de las columnas para ignorar mayúsculas/minúsculas
        project_data.columns = project_data.columns.str.lower()
        extra_data.columns = extra_data.columns.str.lower()

        # Realizar el merge
        return project_data.merge(extra_data, on="sampleid", how="left")

# %%
def process_project(project_id, s3_client, s3_bucket_extra_data, bucket_name):
    project_data = pd.DataFrame(get_metadata_by_project_id(project_id))
    if not project_data.empty:
        file_name = f"{project_id}/{project_id}_extra.csv"
        extra_data = read_from_s3(s3_bucket_extra_data, file_name, s3_client)

        # Asegurarse de que extra_data sea un DataFrame o establecerlo como DataFrame vacío si no es así
        if not isinstance(extra_data, pd.DataFrame):
            logging.error(f'Proyecto Meta {project_id}: {extra_data.get("error", "Error desconocido al leer datos extra.")}')
            extra_data = pd.DataFrame()  # Crea un DataFrame vacío para permitir la creación de full_data

        full_data = merge_data_if_needed(project_data, extra_data)
        meta_parquet = df_to_parquet(full_data)
        upload_project_parquet_to_s3(project_id, meta_parquet, s3_client, bucket_name, 'meta')
        logging.info(f'Proyecto Meta {project_id}: Datos subidos correctamente a S3.')
    else:
        logging.warning(f'Proyecto Meta {project_id}: No se encontraron datos.')

# %%
def upload_parquets_meta_for_projects(project_list, s3_client):
    s3_bucket_extra_data = 'siwaexperiments'
    projects_parquet_key = 'projects/projects_record/meta.parquet'
    bucket_name = "siwaparquets"

    updated_projects_df = read_updated_projects(s3_client, bucket_name, projects_parquet_key)

    for project_id in project_list:
        if project_id in updated_projects_df:
            logging.info(f'Proyecto meta {project_id}: El archivo ya está en S3 y no se subirá nuevamente.')
            continue
        process_project(project_id, s3_client, s3_bucket_extra_data, bucket_name)
    return "Proceso completado con éxito."




