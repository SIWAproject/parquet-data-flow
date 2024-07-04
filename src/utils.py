
# %%
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
from src.base import AnimalPets, AnimalProd,GeneExpression, Histopathology, KitPets, KitProduction, Microbiome, OtuCount, Taxonomy
import logging
from io import BytesIO
from pathlib import Path
import psycopg2
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
logging.basicConfig()
logging.getLogger('sqlalchemy').setLevel(logging.WARNING)

# get the conventional shorthand name of the current checked out branch
checkedout_branch = 'main'
sa_db = "dev" 
project_ids = ['FractalPronaca1']
project_name = project_ids[0]
s3_client = boto3.client('s3')
bucket_name = 'siwaathena'
database_name = "siwa_adb"  
athena_client = boto3.client('athena', region_name='us-east-2')

"""
Create the URL to connect to the specific database namespaced to this branch

If you are on MAIN, there is no namespacing.
For instance, if your branch name is "my_branch", the database name will be "my_branch_dev"
"""

url = URL.create(
    drivername='redshift+redshift_connector',
    host='iluma-kb-1.cn4ff1ztoyt9.us-east-1.redshift.amazonaws.com',
    port=5439,
    database=sa_db,
    username='awsuser',
    password='Ilumasiwa1'
)


    # Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("/////////////////////////////////////////")
logger.info("=====> CONNECTING TO DB {}".format(sa_db))
logger.info("/////////////////////////////////////////")

redshift_engine = sa.create_engine(url)

Session = sessionmaker()
Session.configure(bind=redshift_engine)
session = Session()

redshift_metadata = sa.MetaData(bind=session.bind)


def namespace_table(table_name):
    if checkedout_branch == "main":
        return "{}_{}".format(checkedout_branch, table_name)
    else:
        return table_name


def get_pycon():
    conn = psycopg2.connect(
        dbname=sa_db,
        user='awsuser',
        password='Ilumasiwa1',
        host='iluma-kb-1.cn4ff1ztoyt9.us-east-1.redshift.amazonaws.com',
        port=5439
    )
    return conn


def fetch_kits_prod():
    try:
        # Realiza la consulta 
        kits = session.query(KitProduction).all()
        
        for kit in kits:
            print(f"Kit ID: {kit.kitId}, Project ID: {kit.projectId}, Treatment: {kit.treatment}, Age: {kit.age}, Farm: {kit.farm}, Location: {kit.farmLocation}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()



def fetch_kits_prod_to_dataframe():
    try:
        # Realiza la consulta
        kits = session.query(KitProduction).all()
        
        # Crear una lista de diccionarios, donde cada diccionario representa un kit
        data = [{
            'Kit ID': kit.kitId,
            'Project ID': kit.projectId,
            'Treatment': kit.treatment,
            'Treatment Number': kit.treatmentNumber,
            'Age': kit.age,
            'Farm': kit.farm,
            'Farm Location': kit.farmLocation
        } for kit in kits]
        
        # Convertir la lista de diccionarios a DataFrame
        df = pd.DataFrame(data)
        return df

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

def fetch_pets_kits_to_dataframe():
    try:
        # Suponiendo que existe una sesión de base de datos y una clase de modelo KitPets
        # Realiza la consulta para obtener todos los kits de mascotas
        kits = session.query(KitPets).all()

        # Crear una lista de diccionarios, donde cada diccionario representa un kit de mascota
        data = [{
            'Kit ID': kit.kitId,
            'Project ID': kit.projectId,
            'Grouping Variable': kit.groupingVar,
            'Value': kit.value
        } for kit in kits]

        # Convertir la lista de diccionarios a DataFrame
        df = pd.DataFrame(data)
        return df

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

# Llamar a la función para obtener el DataFrame
df_pets_kits = fetch_pets_kits_to_dataframe()
print(df_pets_kits)


def fetch_animals_prod_to_dataframe():
    try:
        animals = session.query(AnimalProd).all()
        data = [{
            'Animal ID': animal.animalId,
            'Identifier': animal.identifier,
            'Kit ID': animal.kitId,
            'Sex': animal.sex,
            'House': animal.house,
            'Pen': animal.pen,
            'Panels': animal.panels
        } for animal in animals]
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

def fetch_animals_pets_to_dataframe():
    try:
        animals = session.query(AnimalPets).all()
        data = [{
            'Animal Sample ID': animal.animalSampleId,
            'Animal ID': animal.animalId,
            'Kit ID': animal.kitId,
            'Name': animal.name,
            'Location City': animal.locationCity,
            'Breed': animal.breed,
            'Species': animal.species,
            'Age Years': animal.ageYears,
            'Sample Time': animal.sampleTime,
            'Sex': animal.sex
        } for animal in animals]
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()



def fetch_microbiome_to_dataframe():
    try:
        samples = session.query(Microbiome).all()
        data = [{
            'Sample ID': sample.sampleId,
            'Animal ID': sample.animalId,
            'Run ID': sample.runId,
            'Sample Location': sample.sampleLocation,
            'Alpha Shannon': sample.alphaShannon,
            'Alpha Observed': sample.alphaObserved
        } for sample in samples]
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

# Función para exportar los datos a un DataFrame
def export_histopathology_to_dataframe():
    try:
        records = session.query(Histopathology).all()
        data = [record.asdict() for record in records]
        df_histo = pd.DataFrame(data)
        return df_histo
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()


def export_geneexpression_to_csv():
    try:
        df = session.query(GeneExpression).all()
        data = [{
            'Sample ID': item.sampleId,
            'Plate Code': item.plateCode,
            'Animal ID': item.animalId,
            'Sample Location': item.sampleLocation,
            'Target Gene': item.targetGene,
            'Delta Cq': item.deltaCq,
            'Unique Key': item.uniqueKey
        } for item in df]
        df_gen = pd.DataFrame(data)
        return df_gen
        # csv_buffer = StringIO()
        # df.to_csv(csv_buffer, index=False)
        # csv_buffer.seek(0)
        # s3_resource = boto3.resource('s3')
        # bucket_name = 'siwaathena'
        # s3_path = 'geneexpression/geneexpression.csv'
        # s3_object = s3_resource.Object(bucket_name, s3_path)
        # s3_object.put(Body=csv_buffer.getvalue())
        # print(f"File uploaded to S3: s3://{bucket_name}/{s3_path}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()


def check_query_status(query_execution_id):
    result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
    status = result['QueryExecution']['Status']['State']
    return status, result


def fetch_otu_counts_to_dataframe():
    try:
        # Realiza la consulta
        otu_counts = session.query(OtuCount).all()
        
        # Crear una lista de diccionarios, donde cada diccionario representa un conteo OTU
        data = [{
            'Sample ID': otu_count.sampleId,
            'OTU': otu_count.otu,
            'Value': otu_count.value,
            'Unique Key': otu_count.uniqueKey
        } for otu_count in otu_counts]
        
        # Convertir la lista de diccionarios a DataFrame
        df = pd.DataFrame(data)
        return df

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()


# Función para recuperar datos y convertirlos a DataFrame
def fetch_taxonomy_to_dataframe():
    try:
        # Realiza la consulta
        taxonomy_query = session.query(Taxonomy)
        data = [{
            'OTU': tax.otu,
            'Species': tax.species,
            'Genus': tax.genus,
            'Family': tax.family,
            'Order': tax.order,
            'Class': tax.tclass,
            'Phylum': tax.phylum,
            'Kingdom': tax.kingdom
        } for tax in taxonomy_query.all()]
        
        # Convertir la lista de diccionarios a DataFrame
        df = pd.DataFrame(data)
        return df

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        session.close()

