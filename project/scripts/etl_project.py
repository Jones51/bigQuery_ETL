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
import gc
import psutil
import time

# ==============================================================
# Utilitários auxiliares
# ==============================================================
class MemoryManager():
    """
    Classe para gerenciamento de memória.
    
    Métodos:
        - get_memory: verifica a quantidade de memória.
        - clean_memory: limpa a memória.
        - wait_memory: espera até que a memória esteja abaixo do limite definido.
    """
    @staticmethod
    def get_memory():
        try:
            return psutil.virtual_memory().percent
        except Exception as e:
            print(f"Erro ao obter uso de memória: {e}")
            return None
        
    @staticmethod
    def clean_memory(max_percent = 90):
        usage = MemoryManager.get_memory()
        
        if usage is None:
            return False
        
        if usage > max_percent:
            gc.collect()
            time.sleep(0.5)
            print("Memória limpa.")

    @staticmethod
    def wait_memory(max_percent = 90, max_wait_minutes = 5):
        start_wait = time.time()
        max_time_seconds = max_wait_minutes * 60
        
        while True:
            usage = MemoryManager.get_memory()
            if usage is None:
                return True
            
            if usage <= max_percent:
                return True
            
            elapsed_time = (time.time() - start_wait)
            if elapsed_time >= max_time_seconds:
                print("Tempo máximo de espera atingido.")
                return True
            
            print(f"Uso de memória alto ({usage}%). Aguardando...")
            MemoryManager.clean_memory(max_percent)
            time.sleep(5)
        
class RobustAPI():
    """
    Classe para chamadas robustas à API com tentativas e tratamento de erros.
    """
    
    def __init__(self, url, headers):
        self.url = url.strip('/')
        self.headers = headers
        
    def makeRequest(self, endpoint, params=None, max_retries=5):
        attempt = 0
        while attempt < max_retries:
            try:
                full_url = f"{self.url}/{endpoint.lstrip('/')}"
                response = requests.get(full_url, headers=self.headers, params=params)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code in [429, 500, 502, 503, 504]:
                    print(f"Tentativa {attempt+1}/{max_retries} - Erro temporário ({response.status_code}). Tentando novamente...")
                    attempt += 1
                    time.sleep(2 ** attempt)
                else:
                    print(f"Erro ao chamar API: {response.status_code} - {response.text}")
                    response.raise_for_status()
            except requests.RequestException as e:
                print(f"Exceção durante a chamada à API: {e}")
                attempt += 1
                time.sleep(2 ** attempt)

# ==============================================================
# Classe APIs ETL
# ==============================================================
class UserData(RobustAPI):
    """
    Classe para processo de extract e transform da API de usuários.
    """
    def __init__(self, api_url):
       #Controle de memória
       MemoryManager.wait_memory(max_percent=90, max_wait_minutes=5) 
        
       self.api_url = api_url
       self.client = RobustAPI(api_url, headers={})
       
       MemoryManager.clean_memory(max_percent=90)
       
    def fetch_data(self):
        """
        Extrai os dados da API.
        """
        return self.client.makeRequest(endpoint='', params={})
    
    def data_transform(self, data):
        """
        Transforma os dados extraídos da API em um DataFrame e faz tratamentos necessários.
        
        Args:
            data (dict): Dados extraídos da API.
        Returns:
            pd.DataFrame: DataFrame transformado.
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

# ==============================================================
# Classes de Loaders nos Bancos
# ==============================================================
class DataLoader:
    """
    Classe para carregar dados em diferentes bancos de dados.
    """
    def __init__(self, env_path: Optional[str] = None):
        self.config = self.load_config(env_path) 
    
    def load_config(self,env_path: Optional[str] = None) -> Dict[str, str]:
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

    def postgresql_load(self, data_df, config):
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

        data_relacionais.to_sql(
            name='users', 
            con=engine, 
            if_exists='append', 
            index=False,
            chunksize=1000
        )

    def mongodb_load(self, data_df, config):
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

    def bigquery_load(self, data_df, config):
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

    #Inicializando 
    api_url = 'https://randomuser.me/api/'
    user_data_api = UserData(api_url)
    data_loader = DataLoader()
    
    try:
        # Extração dos dados
        if data := user_data_api.makeRequest(endpoint=''):
            print("Dados obtidos com sucesso da API.")
            
            # Transformação dos dados
            data_df = user_data_api.data_transform(data)
            print("Dados transformados com sucesso.")

            # Carregando as configurações do .env
            config = data_loader.config

            # Carregando dados no PostgreSQL
            data_loader.postgresql_load(data_df, config)
            print("Dados carregados com sucesso no PostgreSQL.")

            # Carregando dados no MongoDB
            data_loader.mongodb_load(data_df, config)
            print("Dados carregados com sucesso no MongoDB.")

            # Carregando dados no BigQuery
            data_loader.bigquery_load(data_df, config)
            print("Dados carregados com sucesso no BigQuery.")

            MemoryManager.clean_memory(max_percent=90)
        else:
            print("Sem dados da API.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        MemoryManager.clean_memory(max_percent=90)