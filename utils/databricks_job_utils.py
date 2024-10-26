import requests
import json
from configparser import ConfigParser

class DatabricksJobRunner:
    def __init__(self):
        config = ConfigParser()
        config.read('config.ini')
        self.host = config['databricks']['host']
        self.token = config['databricks']['token']
        self.job_id = config['databricks']['job_id']

    def trigger_job(self):
        """Trigger a Databricks job and return the response"""
        url = f"{self.host}/api/2.1/jobs/run-now"
        headers = {
            'Authorization': f"Bearer {self.token}",
            'Content-Type': 'application/json'
        }
        payload = {
            'job_id': self.job_id
        }

        response = requests.post(url, headers=headers, json=payload)
        return response

    def get_run_status(self, run_id):
        """Get the status of a specific job run"""
        url = f"{self.host}/api/2.1/jobs/runs/get"
        headers = {
            'Authorization': f"Bearer {self.token}",
            'Content-Type': 'application/json'
        }
        params = {
            'run_id': run_id
        }

        response = requests.get(url, headers=headers, params=params)
        return response
