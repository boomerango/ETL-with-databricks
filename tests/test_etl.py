import pytest
import requests
from unittest.mock import Mock, patch
from configparser import ConfigParser

class TestDatabricksJobTrigger:
    @pytest.fixture
    def config(self):
        config = ConfigParser()
        config.read('config.ini')
        return config

    @pytest.fixture
    def databricks_config(self, config):
        return {
            'host': config['databricks']['host'],
            'token': config['databricks']['token'],
            'job_id': config['databricks']['job_id']
        }

    def test_trigger_job_successful(self, databricks_config):
        """Test successful job triggering"""
        with patch('requests.post') as mock_post:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                'run_id': 123456,
                'number_in_job': 1
            }
            mock_post.return_value = mock_response

            url = f"{databricks_config['host']}/api/2.1/jobs/run-now"
            headers = {
                'Authorization': f"Bearer {databricks_config['token']}",
                'Content-Type': 'application/json'
            }
            payload = {
                'job_id': databricks_config['job_id']
            }

            response = requests.post(url, headers=headers, json=payload)

            assert response.status_code == 200
            assert 'run_id' in response.json()
            mock_post.assert_called_once_with(url, headers=headers, json=payload)

    def test_trigger_job_failed_auth(self, databricks_config):
        """Test job triggering with failed authentication"""
        with patch('requests.post') as mock_post:
            mock_response = Mock()
            mock_response.status_code = 403
            mock_response.json.return_value = {
                'error_code': 'INVALID_TOKEN',
                'message': 'Invalid access token'
            }
            mock_post.return_value = mock_response

            url = f"{databricks_config['host']}/api/2.1/jobs/run-now"
            headers = {
                'Authorization': 'Bearer invalid_token',
                'Content-Type': 'application/json'
            }
            payload = {
                'job_id': databricks_config['job_id']
            }

            response = requests.post(url, headers=headers, json=payload)

            assert response.status_code == 403
            assert 'error_code' in response.json()
            assert response.json()['error_code'] == 'INVALID_TOKEN'

    def test_trigger_job_invalid_job_id(self, databricks_config):
        """Test job triggering with invalid job ID"""
        with patch('requests.post') as mock_post:
            mock_response = Mock()
            mock_response.status_code = 400
            mock_response.json.return_value = {
                'error_code': 'INVALID_PARAMETER_VALUE',
                'message': 'Job ID does not exist'
            }
            mock_post.return_value = mock_response

            url = f"{databricks_config['host']}/api/2.1/jobs/run-now"
            headers = {
                'Authorization': f"Bearer {databricks_config['token']}",
                'Content-Type': 'application/json'
            }
            payload = {
                'job_id': 'invalid_job_id'
            }

            response = requests.post(url, headers=headers, json=payload)

            assert response.status_code == 400
            assert 'error_code' in response.json()
            assert response.json()['error_code'] == 'INVALID_PARAMETER_VALUE'
