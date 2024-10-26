from dataclasses import dataclass
import os
from configparser import ConfigParser



@dataclass
class SparkConfig:
    app_name: str = "COVID-19 Vaccination ETL"

def load_config():
    config = ConfigParser()
    config.read('./config.ini')
    return config

@dataclass
class StorageConfig:
    def __init__(self):
        config = load_config()
        self.account_name = config['storage']['account_name']
        self.account_key = config['storage']['account_key']
        self.container_name = config['storage']['container_name']
        
        # Validate immediately
        if not self.account_name:
            raise ValueError(f"STORAGE_ACCOUNT_NAME is None. Current value: {self.account_name}")
        if not self.account_key:
            raise ValueError("STORAGE_ACCOUNT_KEY is None")

    @property
    def blob_url(self) -> str:
        return f"wasbs://{self.container_name}@{self.account_name}.blob.core.windows.net/"

@dataclass
class DataConfig:
    vaccination_data_url: str = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
    dbfs_vaccination_path: str = "dbfs:/FileStore/vaccination_data.csv"
    processed_data_path: str = "processed_data/"
