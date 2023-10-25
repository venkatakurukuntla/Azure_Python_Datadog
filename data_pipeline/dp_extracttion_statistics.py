#######################################################################################################################
## File Name: py_process_dp_extracttion_statistics_data.py                                                            #
## Description: Python Script for pushing the Extract Data Pipeline Statistics into Datadog                           #
##                                                                                                                    #
## Creation Date: 2023-10-03                                                                                          #
## Created By: TEKsystems Datadog Development Team                                                                    #
## Last Modified Date: 2023-10-03                                                                                     #
## Last Modified By: TEKsystems Datadog Development Team                                                              #
#######################################################################################################################

import json
import requests
import azure.functions as func

from pytz import timezone as py_tz
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime

# Function to Load the Data Pipeline Stats into Datadog
def f_dp_stats_gathering(req_body):
    try:
        dp_end_time = str(datetime.now(py_tz("US/Eastern")))[:-6][:23]
        if len(dp_end_time) == 19: dp_end_time = dp_end_time + '.000'
        # Get DP Variables from the req_body Parameter
        dp_name = req_body["pipe_line_name"]
        dp_container = req_body["container"]
        dp_directory = req_body["directory"]
        dp_status = req_body["status"]
        dp_start_time = req_body["job_start_time"]
        # Retrieve Azure Key Vault url
        response = open("config.json", "r")
        content = response.read()
        content = json.loads(content)
        azure_keyvault_url = content['key_vault_url']
        # Get Secrets and Container Client
        secret_name_blob = dp_container.capitalize()+"ConnectionString"
        secret_name_api = "DatadogAPIKey"
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url = azure_keyvault_url, credential = credential)
        blob_connection_string = secret_client.get_secret(secret_name_blob).value
        datadog_api_key = secret_client.get_secret(secret_name_api).value
        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        container_client = blob_service_client.get_container_client(dp_container)
        # Calculate Times
        dp_start_time_dttm = datetime.strptime(dp_start_time, '%Y-%m-%d %H:%M:%S.%f')
        dp_end_time_dttm = datetime.strptime(dp_end_time, '%Y-%m-%d %H:%M:%S.%f')
        dp_end_time_num = dp_end_time_dttm.strftime("%Y%m%d%H%M%S")
        # Extract config.json Data to Get Header Details
        blob_name_dp_config = f"{dp_directory}/logic/config.json"
        blob_client = container_client.get_blob_client(blob_name_dp_config)
        blob_client_download = blob_client.download_blob()
        blob_client_readall = blob_client_download.readall()
        blob_dp_config = json.loads(blob_client_readall)
        # Create the Required JSON Tree Structure
        job_data_dd = {
            "ddsource":blob_dp_config.get('ddsource'),
            "hostname":blob_dp_config.get('hostname'),
            "service":blob_dp_config.get('service'),
            "data_pipeline":
                {"custom":
                    {"job":
                        {"id":f"{dp_name} {dp_start_time}",
                        "name":dp_name,
                        "start_time":dp_start_time,
                        "end_time":dp_end_time,
                        "duration":(dp_end_time_dttm - dp_start_time_dttm).total_seconds(),
                        "status":dp_status},
                    "header":
                        {"timestamp":dp_end_time,
                        "env":blob_dp_config.get('env'),
                        "batch_id":int(dp_end_time_num),
                        "application":dp_container.capitalize(),
                        "object":dp_directory.capitalize()[:-9]
                        }
                    }
                }
            }
        # Push the JSON Data to Datadog
        datadog_url = blob_dp_config.get('dd_url')
        headers = {
            'Accept': 'application/json',
            'DD-API-KEY': datadog_api_key,
            'Content-Type': 'application/json'
            }
        requests.post(datadog_url, headers = headers, json = job_data_dd)
        # Archive the JSON Data to /pipeline/archive/ Directory
        blob_name = f"{dp_directory}/archive/{dp_name}_{dp_end_time_num}.json"
        job_data_dd_fomatted = json.dumps(job_data_dd)
        container_client.upload_blob(data = job_data_dd_fomatted, name = blob_name, overwrite = True)
        return "Success"
    except Exception as e:
        print('Error: ', e)
        return e

# Function Main
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        response = f_dp_stats_gathering(req_body)
        return func.HttpResponse(f"{response}")
    except Exception as e:
        return func.HttpResponse(f"{e}")