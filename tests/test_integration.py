import pytest
import requests
from configparser import ConfigParser
import time
from azure.storage.blob import BlobServiceClient
import pyarrow.parquet as pq
import io
import great_expectations as gx
import pandas as pd

config = ConfigParser()
config.read('config.ini')

DATABRICKS_HOST = config.get('databricks', 'host')
DATABRICKS_TOKEN = config.get('databricks', 'token')
JOB_ID = config.get('databricks', 'job_id')

@pytest.mark.usefixtures("databricks_runner", "storage_client")
class TestDatabricksJobExecution:

    @pytest.mark.dependency()
    def test_end_to_end_job_execution(self, databricks_runner, storage_client):
        """Test the complete end-to-end job execution flow including storage verification"""
        response = databricks_runner.trigger_job()
        assert response.status_code == 200, "Failed to trigger job"
        run_id = response.json()['run_id']
        assert run_id is not None, "Failed to get run_id"

        max_wait_time = 600
        start_time = time.time()
        
        while True:
            if time.time() - start_time > max_wait_time:
                pytest.fail("Job execution timed out")

            response = databricks_runner.get_run_status(run_id)
            state = response.json()['state']
            life_cycle_state = state['life_cycle_state']
            
            if life_cycle_state == 'TERMINATED':
                result_state = state['result_state']
                assert result_state == 'SUCCESS', f"Job failed with state: {state}"
                break
            elif life_cycle_state in ['RUNNING', 'PENDING']:
                time.sleep(30)
            else:
                pytest.fail(f"Unexpected job state: {state}")

        self._verify_parquet_files(storage_client)

    def _verify_parquet_files(self, storage_client):
        """Helper method to verify Parquet files in storage"""
        container_name = "etl-demo"
        container_client = storage_client.get_container_client(container_name)
        
        output_directory = "processed_data"
        blobs = list(container_client.list_blobs(name_starts_with=output_directory))
        
        assert len(blobs) > 0, "No processed files found in storage account"
        
        parquet_files = [blob for blob in blobs if blob.name.endswith('.parquet')]
        assert len(parquet_files) > 0, "No Parquet files found in processed data"
        
        latest_parquet = max(parquet_files, key=lambda x: x.last_modified)
        blob_client = container_client.get_blob_client(latest_parquet.name)
        content = blob_client.download_blob().readall()
        parquet_file = pq.ParquetFile(io.BytesIO(content))
        
        schema = parquet_file.schema
        assert 'iso_code' in str(schema), "Missing required column in Parquet file"
        assert 'total_vaccinations' in str(schema), "Missing required column in Parquet file"

    @pytest.mark.dependency(depends=["TestDatabricksJobExecution::test_end_to_end_job_execution"])
    def test_data_quality_validation(self, storage_client):
        """Test data quality using Great Expectations after ETL job execution"""
        container_name = "etl-demo"
        container_client = storage_client.get_container_client(container_name)
        
        output_directory = "processed_data"
        blobs = list(container_client.list_blobs(name_starts_with=output_directory))
        parquet_files = [blob for blob in blobs if blob.name.endswith('.parquet')]
        latest_parquet = max(parquet_files, key=lambda x: x.last_modified)
        
        blob_client = container_client.get_blob_client(latest_parquet.name)
        content = blob_client.download_blob().readall()
        df = pd.read_parquet(io.BytesIO(content))
        
        context = gx.get_context()
        data_source = context.data_sources.add_pandas(name="my_datasource")
        data_asset = data_source.add_dataframe_asset(name="vaccination_data")
        
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            "my_batch_definition"
        )
        
        batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
        
        expectations_to_test = [
            gx.expectations.ExpectTableColumnCountToEqual(value=67),
            gx.expectations.ExpectTableColumnsToMatchSet(
                column_set=[
                    'iso_code', 'continent', 'location', 'date',
                    'total_cases', 'new_cases', 'new_cases_smoothed',
                    'total_deaths', 'new_deaths', 'new_deaths_smoothed',
                    'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated'
                ]
            ),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="iso_code"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="date"),
            gx.expectations.ExpectColumnValuesToMatchRegex(
                column="date",
                regex=r"^\d{4}-\d{2}-\d{2}$"
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="total_vaccinations",
                min_value=0,
                max_value=10000000000,
                mostly=0.95
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="total_cases",
                min_value=0,
                max_value=1000000000,
                mostly=0.95
            ),
            gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A="total_vaccinations",
                column_B="people_vaccinated",
                or_equal=True,
                mostly=0.90
            ),
            gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A="people_vaccinated",
                column_B="people_fully_vaccinated",
                or_equal=True,
                mostly=0.95
            ),
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=['date', 'iso_code']
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="life_expectancy",
                min_value=20,
                max_value=100,
                mostly=0.95
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="population",
                min_value=1000,
                mostly=0.95
            )
        ]
        
        for expectation in expectations_to_test:
            validation_result = batch.validate(expectation)
            print(f"\nValidating: {expectation.__class__.__name__}")
            print(f"Success: {validation_result.success}")
            if not validation_result.success:
                print(f"Details: {validation_result.result}")
            assert validation_result.success, f"Failed validation for {expectation.__class__.__name__}"
        
        summary_stats = df.describe()
        total_rows = len(df)
        assert total_rows > 0, "Dataset is empty"
