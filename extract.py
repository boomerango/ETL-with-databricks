import subprocess

def install_packages():
    subprocess.check_call(["pip", "install", "-r", "requirements.txt"])

install_packages()
########################################################
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import col
from config import SparkConfig, StorageConfig, DataConfig

class CovidVaccinationETL:
    def __init__(self):
        self.spark_config = SparkConfig()
        self.storage_config = StorageConfig()
        self.data_config = DataConfig()
        self.spark = SparkSession.builder.appName(self.spark_config.app_name).getOrCreate()
        self.configure_blob_storage()
        
    def configure_blob_storage(self):
        """Configure Azure Blob Storage connection"""
        self.spark.conf.set(
            f"fs.azure.account.key.{self.storage_config.account_name}.blob.core.windows.net",
            self.storage_config.account_key
        )
        self.blob_url = self.storage_config.blob_url

    def download_data_to_dbfs(self):
        """Download data from web URL to DBFS"""
        dbutils.fs.cp(self.data_config.vaccination_data_url,  # noqa: F821
                     self.data_config.dbfs_vaccination_path)

    def load_data(self):
        """Load data from DBFS into Spark DataFrame"""
        self.vaccination_df = self.spark.read.csv(
            self.data_config.dbfs_vaccination_path,
            header=True,
            inferSchema=True
        )
        return self.vaccination_df

    def clean_data(self):
        """Clean the vaccination data"""
        # Drop rows with null values for non-numeric columns
        self.cleaned_df = self.vaccination_df.na.drop(
            subset=["iso_code", "continent", "location", "date"]
        )

        # Fill numeric columns with median values
        numeric_columns = [
            column[0] for column in self.cleaned_df.dtypes 
            if column[1] in ['int', 'double', 'long']
        ]
        
        for column in numeric_columns:
            median_value = self.cleaned_df.approxQuantile(column, [0.5], 0)[0]
            self.cleaned_df = self.cleaned_df.na.fill({column: median_value})
            
        return self.cleaned_df

    def visualize_data(self):
        """Create visualization of the data"""
        vaccination_pd_df = self.cleaned_df.toPandas()
        vaccination_pd_df = vaccination_pd_df[['total_vaccinations', 'population']].dropna()

        plt.figure(figsize=(10, 6))
        plt.scatter(
            vaccination_pd_df['population'],
            vaccination_pd_df['total_vaccinations'],
            alpha=0.5,
            color='b'
        )
        plt.title('Total Vaccinations vs. Population')
        plt.xlabel('Population')
        plt.ylabel('Total Vaccinations')
        plt.grid(True)
        plt.show()

    def save_to_blob(self):
        """Save processed data to Azure Blob Storage"""
        self.cleaned_df.write.format("parquet").mode("overwrite").save(
            self.blob_url + self.data_config.processed_data_path
        )

    def run_pipeline(self):
        """Execute the complete ETL pipeline"""
        self.download_data_to_dbfs()
        self.load_data()
        self.clean_data()
        self.visualize_data()
        self.save_to_blob()


if __name__ == "__main__":
    etl = CovidVaccinationETL()
    etl.run_pipeline()
