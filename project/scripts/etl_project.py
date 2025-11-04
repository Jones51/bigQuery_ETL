import os
import json
from pathlib import Path
from typing import Dict, Optional
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pymongo
from pymongo import MongoClient
from google.cloud import bigquery

def load_config(env_path: Optional[str] = None) -> Dict[str, str]:
    """
    Load environment variables from a .env file and return them as a dictionary.

    Args:
        env_path (Optional[str]): Path to the .env file. If not provided, searches in the current or parent directory.

    Returns:
        Dict[str, str]: Dictionary containing database and API credentials.
    """
    if env_path:
        load_dotenv(env_path)
    else:
        # procura .env no diretório atual / parent
        load_dotenv()

    return {
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_PORT": os.getenv("DB_PORT"),
        "DB_NAME": os.getenv("DB_NAME"),
        "MONGO_USER": os.getenv("MONGO_USER"),
        "MONGO_PASSWORD": os.getenv("MONGO_PASSWORD"),
        "MONGO_HOST": os.getenv("MONGO_HOST"),
        "MONGO_NAME" : os.getenv("MONGO_NAME"),
        "MONGO_PORT": os.getenv("MONGO_PORT"),
        "MONGO_AUTH_SOURCE": os.getenv("MONGO_AUTH_SOURCE"),
        "BIG_QUERY_DATASET" : os.getenv("BIG_QUERY_DATASET"),
        "BIG_QUERY_TABLE_ID" : os.getenv("BIG_QUERY_TABLE_ID"),
        "BIG_QUERY_PROJECT_ID" : os.getenv("BIG_QUERY_PROJECT_ID"),
    }

def api_etl():
    """
    Extract data from a public API, transform it, and load it into PostgreSQL, MongoDB, and BigQuery.
    """
    try:
        response = requests.get('https://randomuser.me/api/')
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        data = {"results": []}

    return data

def data_transformation(data):
    """
    Transform the raw data from the API into a pandas DataFrame.

    Args:
        data (dict): Raw data from the API.

    Returns:
        pd.DataFrame: Transformed DataFrame.
    """
    data_df = pd.json_normalize(data['results'])

    # Tratando o nome das colunas
    data_df.columns = (
        data_df.columns
        .str.replace('.', '_', regex=False)
        .str.lower()
        .str.strip()
    )

    # Convertendo os valores numéricos para string
    numeric_columns = data_df.select_dtypes(include=['int64', 'float64']).columns
    data_df[numeric_columns] = data_df[numeric_columns].astype(str)

    return data_df

def postgresql_load(data_df, config):
    """
    Load the DataFrame into a PostgreSQL database.

    Args:
        data_df (pd.DataFrame): DataFrame to be loaded.
        config (dict): Database configuration.
    """
    DB_HOST = config["DB_HOST"]
    DB_USER = config["DB_USER"]
    DB_PASSWORD = config["DB_PASSWORD"]
    DB_NAME = config["DB_NAME"]
    DB_PORT = config["DB_PORT"]

    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(DATABASE_URL)

    colunas_relacionais = ['email','name_title', 'name_first', 'name_last', 'location_street_number', 'location_street_name','location_city', 'location_state', 'location_country', 'location_postcode']
    data_relacionais = data_df[colunas_relacionais].copy()
    data_relacionais.head(5)

    data_relacionais.to_sql(
        name='users', 
        con=engine, 
        if_exists='append', 
        index=False,
        chunksize=1000
    )

def mongodb_load(data_df, config):
    """
    Load the DataFrame into a MongoDB database.

    Args:
        data_df (pd.DataFrame): DataFrame to be loaded.
        config (dict): Database configuration.
    """
    USERNAME = config["MONGO_USER"]
    PASSWORD = config["MONGO_PASSWORD"]
    HOST = config["MONGO_HOST"]
    PORT = config["MONGO_PORT"]
    DATABASE_NAME = config["MONGO_NAME"]
    AUTH_SOURCE = config["MONGO_AUTH_SOURCE"]

    connection_string = f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE_NAME}?authSource={AUTH_SOURCE}"

    client = MongoClient(connection_string)
    db = client[DATABASE_NAME]
    collection = db['users']

    records = data_df.to_dict('records')
    collection.insert_many(records)

def bigquery_load(data_df, config):
    """
    Load the DataFrame into a BigQuery table.

    Args:
        data_df (pd.DataFrame): DataFrame to be loaded.
        config (dict): Database configuration.
    """
    client = bigquery.Client()

    DATASET_ID = config["BIG_QUERY_DATASET"]
    TABLE_ID = config["BIG_QUERY_TABLE_ID"]
    PROJECT_ID = client.project

    table_id_completo = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", 
    )

    job = client.load_table_from_dataframe(
        data_df, 
        table_id_completo, 
        job_config=job_config
    ) 

    job.result()  

    table = client.get_table(table_id_completo)

if __name__ == "__main__":

    if data := api_etl():
        print("Dados obtidos com sucesso da API.")
        
        # Transformação dos dados
        data_df = data_transformation(data)
        print("Dados transformados com sucesso.")

        # Carregando as configurações do .env
        config = load_config()

        # Carregando dados no PostgreSQL
        postgresql_load(data_df, config)
        print("Dados carregados com sucesso no PostgreSQL.")

        # Carregando dados no MongoDB
        mongodb_load(data_df, config)
        print("Dados carregados com sucesso no MongoDB.")

        # Carregando dados no BigQuery
        bigquery_load(data_df, config)
        print("Dados carregados com sucesso no BigQuery.")

    else:
        print("Sem dados da API.")