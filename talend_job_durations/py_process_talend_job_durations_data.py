#######################################################################################################################
## File Name: pl_process_talend_job_durations_data.py                                                                 #
## Description: Python Script for Transformation of Talend Job Durations Data                                         #
##                                                                                                                    #
## Creation Date: 2023-09-26                                                                                          #
## Created By: TEKsystems Datadog Development Team                                                                    #
## Last Modified Date: 2023-09-26                                                                                     #
## Last Modified By: TEKsystems Datadog Development Team                                                              #
#######################################################################################################################

import json
import math
import requests
import numpy
import azure.functions as func

from pytz import timezone as py_tz
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime

# Define Blobs and Variables
blob_name_input_wildcard = "job_durations/data/job_durations_inputs_"
blob_name_config = 'job_durations/logic/config.json'
blob_name_job_duration_90p = 'job_durations/data/JOB_DURATION_90p.json'
secret_name_blob = "TalendConnectionString"
secret_name_api = "DatadogAPIKey"
container = "talend"

# Function to Get Secrets and Container Client
def f_get_secret(azure_keyvault_url):
    try:
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=azure_keyvault_url, credential=credential)
        blob_connection_string = secret_client.get_secret(secret_name_blob).value
        datadog_api_key = secret_client.get_secret(secret_name_api).value
        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        container_client = blob_service_client.get_container_client(container)
        return blob_connection_string, datadog_api_key, container_client
    except Exception as e:
        return e, ''

# Call Function to Get Secrets and Container Client
response = open("config.json", "r")
content = response.read()
content = json.loads(content)
key_vault_url = content['key_vault_url']
blob_connection_string, datadog_api_key, container_client = f_get_secret(key_vault_url)

# Function to Read Blobs
def f_read_blobs(blob_name):
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_client_download = blob_client.download_blob()
        blob_client_readall = blob_client_download.readall()
        blob = json.loads(blob_client_readall)
        return blob
    except Exception as e:
        return e

# Funcion to Calculate the 90th Percentile of Job Durations for Each Talend Task_ID
def f_caculate_90p():
    try:
        # Merge the Individual Job Durations Files        
        blob_list = container_client.list_blobs(name_starts_with = blob_name_input_wildcard)
        job_durations_merged = {"items": []}
        for blob in blob_list:
            blob_name_job_durations = blob.name
            if blob_name_job_durations.endswith(".json"):
                blob_job_durations = f_read_blobs(blob_name_job_durations)
                for item in blob_job_durations.get("items", []):
                    task_id = item.get("taskId")
                    start_timestamp = item.get("startTimestamp")
                    finish_timestamp = item.get("finishTimestamp")
                    if task_id and start_timestamp and finish_timestamp:
                        start_time = datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                        finish_time = datetime.strptime(finish_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                        job_duration = math.trunc(((finish_time - start_time).total_seconds())*1000)/1000
                        job_durations_merged_record = {
                            "TASKID": task_id,
                            "JOB_DURATION": job_duration
                        }
                        job_durations_merged["items"].append(job_durations_merged_record)
        # Calculate 90th Percentile        
        job_durations_data = {}
        for item in job_durations_merged["items"]:
            task_id = item["TASKID"]
            job_duration = item["JOB_DURATION"]
            if task_id not in job_durations_data:
                job_durations_data[task_id] = []
            job_durations_data[task_id].append(job_duration)
        job_durations_90p_data=[]
        for task_id, durations in job_durations_data.items():
            result_90p = math.trunc((numpy.percentile(durations, 90))*1000)/1000
            result_90p = {"TASKID":task_id,"JOB_DURATION90p":result_90p}
            job_durations_90p_data.append(result_90p)
        # Archive and Write JSON Files
        suffix = str(datetime.now(py_tz("US/Eastern")).strftime("%Y%m%d%H%M%S"))
        blob_name = f"job_durations/data/job_durations_90p.json"
        blob_name_archive = f"job_durations/data/output_archive/job_durations_90p_{suffix}.json"
        blob_job_durations_90p_old = f_read_blobs(blob_name)
        blob_job_durations_90p_old = json.dumps(blob_job_durations_90p_old)
        blob_job_durations_90p_new = json.dumps(job_durations_90p_data)
        container_client.upload_blob(data = blob_job_durations_90p_old, name = blob_name_archive, overwrite = True)
        container_client.delete_blob(blob = blob_name)
        container_client.upload_blob(data = blob_job_durations_90p_new, name = blob_name, overwrite = True)
        return 'Success'
    except Exception as e:
        return e

# Function to Load the Data Pipeline Stats into Datadog
def f_dp_stats_gathering(dp_name, dp_container, dp_directory, dp_status, dp_start_time):
    try:
        dp_end_time = str(datetime.now(py_tz("US/Eastern")))[:-9]
        # Calculate Times
        dp_start_time_dttm = datetime.strptime(dp_start_time, '%Y-%m-%d %H:%M:%S.%f')
        dp_end_time_dttm = datetime.strptime(dp_end_time, '%Y-%m-%d %H:%M:%S.%f')
        dp_end_time_num = dp_end_time_dttm.strftime("%Y%m%d%H%M%S")
        # Extract config.json Data to Get Header Details
        blob_name_dp_config = f"{dp_directory}/logic/config.json"
        blob_dp_config = f_read_blobs(blob_name_dp_config)
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
# if __name__ == "__main__":
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        dp_start_time =  str(datetime.now(py_tz("US/Eastern")))[:-9]
        # Call the Function to Calculate 90th Percentile for Job Durations
        response_f_transform = f_caculate_90p()
        f_dp_stats_gathering('pl_transform_talend_job_durations_data','talend','job_durations/pipeline','Success', dp_start_time)
        print(response_f_transform)
        return func.HttpResponse(f"{response_f_transform}")
    except Exception as e:
        f_dp_stats_gathering('pl_transform_talend_job_durations_data','talend','job_durations/pipeline','Failure', dp_start_time)
        print(e)
        return func.HttpResponse(f"{e}")