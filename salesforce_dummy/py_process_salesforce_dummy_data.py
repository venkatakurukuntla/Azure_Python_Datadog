#######################################################################################################################
## File Name: py_process_salesforce_dummy_data.py                                                                     #
## Description: Python Script for pushing the dummy date details into Datadog                                         #
##                                                                                                                    #
## Creation Date: 2023-10-11                                                                                          #
## Created By: TEKsystems Datadog Development Team                                                                    #
## Last Modified Date: 2023-10-11                                                                                     #
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
def f_dummy(ct):
    try:
        # Retrieve Azure Key Vault url
        response = open("config.json", "r")
        content = response.read()
        content = json.loads(content)
        azure_keyvault_url = content['key_vault_url']
        # Get Secrets and Container Client
        container = "salesforce"
        secret_name_blob = "SalesforceConnectionString"
        secret_name_api = "DatadogAPIKey"
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url = azure_keyvault_url, credential = credential)
        blob_connection_string = secret_client.get_secret(secret_name_blob).value
        datadog_api_key = secret_client.get_secret(secret_name_api).value
        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        container_client = blob_service_client.get_container_client(container)
        # Calculate Times
        ct_num = ct.strftime("%Y%m%d%H%M%S")
        ct_date_id = ct.strftime("%Y%m%d")
        ct_year = ct.strftime("%Y")
        ct_month = ct.strftime("%m")
        ct_date = ct.strftime("%d")
        ct_timestamp = str(ct)[:-6][:23]
        if len(ct_timestamp) == 19: ct_timestamp = ct_timestamp + '.000'
        if ct.weekday() == 0:
            ct_day = "MON"
        elif ct.weekday() == 1:
            ct_day = "TUE"
        elif ct.weekday() == 2:
            ct_day = "WED"
        elif ct.weekday() == 3:
            ct_day = "THU"
        elif ct.weekday() == 4:
            ct_day = "FRI"
        elif ct.weekday() == 5:
            ct_day = "SAT"
        else:
            ct_day = "SUN"
        # Extract config.json Data to Get Header Details
        blob_name_dp_config = f"dummy/logic/config.json"
        blob_client = container_client.get_blob_client(blob_name_dp_config)
        blob_client_download = blob_client.download_blob()
        blob_client_readall = blob_client_download.readall()
        blob_dp_config = json.loads(blob_client_readall)
        # Create the Required JSON Tree Structure
        job_data_dd = {
            "ddsource":blob_dp_config.get('ddsource'),
            "hostname":blob_dp_config.get('hostname'),
            "service":blob_dp_config.get('service'),
            "salesforce":
                {
                    "custom":
                    {
                    "dummy":
                    {
                    "header":
                        {
                        "timestamp":ct_timestamp,
                        "env":blob_dp_config.get('env'),
                        "batch_id":int(ct_num),
                        "date_id":int(ct_date_id)
                        },
                    "yyyy":int(ct_year),
                    "mm":int(ct_month),
                    "dd":int(ct_date),
                    "ddd":ct_day,
                    "count":1,
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
        return "Success"
    except Exception as e:
        print('Error: ', e)
        return e

# Function Main
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        response = "It's a weekend."
        ct = datetime.now(py_tz("US/Eastern"))
        if ct.weekday() < 5:
            response = f_dummy(ct)
        return func.HttpResponse(f"{response}")
    except Exception as e:
        return func.HttpResponse(f"{e}")