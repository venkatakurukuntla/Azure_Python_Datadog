#Script for testing Write on Blob operation 
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pytz import timezone as py_tz
from datetime import datetime

# Function Main
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:

        azure_keyvault_url = "https://corpit-dev-datadg-kv-use.vault.azure.net/"
        secret_name_blob = "TalendConnectionString"
        container = "talend"

        v_blob_name = "test/test.json"
        v_job_data = {
            "job_name": "test",
            "job_start_time": "2023-09-21 07:51:04.313"
        }
        v_json_data = json.dumps(v_job_data)
        
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url = azure_keyvault_url, credential = credential)
        blob_connection_string = secret_client.get_secret(secret_name_blob).value
        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        container_client = blob_service_client.get_container_client(container)

        response = container_client.upload_blob(data=v_json_data, name=v_blob_name, overwrite=True)

        return func.HttpResponse(f"Response : - {response}")
    except Exception as e:
        return func.HttpResponse(f"Exception : - {e}")
