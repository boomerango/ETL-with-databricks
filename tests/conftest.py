import pytest
from utils.databricks_job_utils import DatabricksJobRunner
from azure.storage.blob import BlobServiceClient
from configparser import ConfigParser

@pytest.fixture(scope="class")
def databricks_runner():
    """Fixture that provides a DatabricksJobRunner instance"""
    return DatabricksJobRunner()

@pytest.fixture(scope="class")
def storage_client():
    """Fixture that provides an Azure Storage Client"""
    config = ConfigParser()
    config.read('config.ini')
    
    account_name = config.get('storage', 'account_name')
    account_key = config.get('storage', 'account_key')
    
    return BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key
    )
