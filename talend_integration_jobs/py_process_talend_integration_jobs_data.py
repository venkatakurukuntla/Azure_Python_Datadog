#######################################################################################################################
## File Name: pl_process_talend_integration_jobs_data.py                                                              #
## Description: Python Script for Transformation of Talend Integration Jobs Data and Load into Datadog                #
##                                                                                                                    #
## Creation Date: 2023-09-25                                                                                          #
## Created By: TEKsystems Datadog Development Team                                                                    #
## Last Modified Date: 2023-09-25                                                                                     #
## Last Modified By: TEKsystems Datadog Development Team                                                              #
#######################################################################################################################

import json
import pytz
import math
import requests
import azure.functions as func

from pytz import timezone as py_tz
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime

# Define Blobs and Variables
blob_name_executions_wildcard = "executions/data/executions"
blob_name_tasks = "tasks/data/tasks.json"
blob_name_timestamp = "integration_jobs/data/timestamp.json"
blob_name_integration_jobs = "integration_jobs/data/integration_jobs.json"
blob_name_config = 'integration_jobs/logic/config.json'
blob_name_job_duration_90p = 'job_durations/data/job_durations_90p.json'
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
    
def get_latest_talendblob(container_name,cs):
    blob_name1="executions/data/input_archive/executions_"
    # Get a list of blobs in the container
    json_blobs=[]
    blob_service_client = BlobServiceClient.from_connection_string(cs)
    container_client = blob_service_client.get_container_client("talend")
    blob_list = container_client.list_blobs(name_starts_with=blob_name1)
    for blob in blob_list:
        if blob.name.endswith(".json"):
            json_blobs.append(blob.name)

    
    if not json_blobs:
        return None
    # Get the latest blob based on timestamp
    latest_blob_name = max(json_blobs)
    return latest_blob_name

# Function to Transform the Extracted JSON Data
def f_transform_talend_data(blob_executions):
    try:
        # Retrieve executions.json Records to Fix the 'Missing Fileds' Issue
        executions_data = []
        for item in blob_executions['items']:
        #for item in blob_executions:
            item_data = {}
            for field in ['startTimestamp','finishTimestamp','taskId','executionType','status','errorMessage','executionId']:
                if field in item:
                    item_data[field] = item[field]
                else:
                    item_data[field] = ''
            executions_data.append(item_data)
        # Extract tasks.json Data to Get Name Fields - jobname, projectname, environment
        blob_tasks = f_read_blobs(blob_name_tasks)
        tasks_data = {}
        # record={}
        #for item in blob_executions['items']:

        for item in blob_tasks['items']:

                executable = item['executable']
                task_name = item['name']
                workspace_name = item['workspace']['name']
                environment_name = item['workspace']['environment']['name']
                tasks_data[executable] = {
                    'JOBNAME': task_name,
                    'PROJECTNAME': workspace_name,
                    'ENVIRONMENT': environment_name
                }
    
        # with open("tasks.json",'w') as f:
        #     json.dump(tasks_data,f,indent=4)
        # Extract timestamp.json Data to Get Timestamp Fields
        json_timestamp = f_read_blobs(blob_name_timestamp).get('timestamp')
        json_batch_id = int(datetime.strptime(json_timestamp, "%Y-%m-%d %H:%M:%S.%f").strftime("%Y%m%d%H%M%S"))
        # Extract config.json Data to Get Header Details
        blob_config = f_read_blobs(blob_name_config)
        config_ddsouce = blob_config.get('ddsource')
        config_hostname = blob_config.get('hostname')
        config_service = blob_config.get('service')
        config_env = blob_config.get('env')
        # Create Integration Jobs Data in JSON Format
        for item in executions_data:
            executable = item['taskId']
            if executable in tasks_data:
                tasks_info = tasks_data[executable]
                start_timestamp_in = item.pop('startTimestamp')
                start_timestamp = str(datetime.strptime(start_timestamp_in, "%Y-%m-%dT%H:%M:%S.%f%z").astimezone(pytz.timezone('US/Eastern')))[:-6][:23]
                if len(start_timestamp) == 19: start_timestamp = start_timestamp + '.000'
                start_timestamp_dttm = datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S.%f")
                
                finish_timestamp_in = item.pop('finishTimestamp')
                finish_timestamp = json_timestamp
                if finish_timestamp_in != '':
                    finish_timestamp = str(datetime.strptime(finish_timestamp_in, "%Y-%m-%dT%H:%M:%S.%f%z").astimezone(pytz.timezone('US/Eastern')))[:-6][:23]
                if len(finish_timestamp) == 19: finish_timestamp = finish_timestamp + '.000'
                finish_timestamp_dttm = datetime.strptime(finish_timestamp, "%Y-%m-%d %H:%M:%S.%f")
                # Assign the Values to the JSON Tags
                item['JOBNAME'] = tasks_info['JOBNAME']
                item['PROJECTNAME'] = tasks_info['PROJECTNAME']
                item['ENVIRONMENT'] = tasks_info['ENVIRONMENT']
                item['START_TIME'] = start_timestamp
                item['END_TIME'] = finish_timestamp
                item['TASKID'] = item.pop('taskId')
                item['EXECUTIONTYPE'] = item.pop('executionType')
                item['STATUS'] = item.pop('status')
                item['ERROR_MESSAGE'] = item.pop('errorMessage')
                item['EXECUTIONID'] = item.pop('executionId')
                item['DDSOURCE'] = config_ddsouce
                item['HOSTNAME'] = config_hostname
                item['SERVICE'] = config_service
                item['ENV'] = config_env
                item['BATCH_ID'] = json_batch_id
                item['TIMESTAMP'] = json_timestamp
                item['JOB_DURATION'] = (finish_timestamp_dttm - start_timestamp_dttm).total_seconds()
       
        integration_jobs_stg_data = {"items": executions_data}

        # Extract Job Duration 90p Data        
        blob_job_duration_90p = f_read_blobs(blob_name_job_duration_90p)
        job_duration_90p_data = {item["TASKID"]: item["JOB_DURATION90p"] for item in blob_job_duration_90p}
        for item in integration_jobs_stg_data["items"]:
            task_id = item["TASKID"]
            if task_id in job_duration_90p_data:
                job_duration_90p = math.trunc(job_duration_90p_data[task_id]*1000)/1000
                overshot_by = math.trunc((item['JOB_DURATION'] - job_duration_90p)*1000)/1000
                item["JOB_DURATION_90p"] = job_duration_90p
                item['OVERSHOT_BY'] = overshot_by
        # Create the Required JSON Tree Structure
        integration_jobs_data = []
        for item in integration_jobs_stg_data["items"]:
            integration_jobs_record = {
                "ddsource":item["DDSOURCE"],
                "hostname":item["HOSTNAME"],
                "service":item["SERVICE"],
                "talend":{
                    "custom":{
                        "integration_job":{
                            "header":{
                                "env":item["ENV"],
                                "batch_id":item["BATCH_ID"],
                                "timestamp":item["TIMESTAMP"],
                                },
                                "execution_type":item["EXECUTIONTYPE"],
                                "start_time":item["START_TIME"],
                                "status":item["STATUS"],
                                "end_time":item["END_TIME"],
                                "error_message":item["ERROR_MESSAGE"],
                                "execution_id":item["EXECUTIONID"],
                                "duration":item["JOB_DURATION"],
                                "duration_threshold":item["JOB_DURATION_90p"],
                                "overshot_by":item["OVERSHOT_BY"],
                                "name":item["JOBNAME"],
                                "project":item["PROJECTNAME"],
                            }
                        }
                    }
                }
            integration_jobs_data.append(integration_jobs_record)
        # Generate the File 'integration_jobs.json'
        # file_path = "./integration_jobs.json"
        # with open(file_path,'w') as f:
        #     json.dump(integration_jobs_data, f, indent = 4)
 
        return integration_jobs_data

    except Exception as e:
        return e

# Function to Load the Transformed JSON Data into Datadog
def f_load_talend_data(blob_name_executions, blob_executions, transformed_data):
    try:
        datadog_url = f_read_blobs(blob_name_config).get('dd_url')
        headers = {
        'Accept': 'application/json',
        'DD-API-KEY': datadog_api_key,
        'Content-Type': 'application/json'
        }
        requests.post(datadog_url, headers = headers, json = transformed_data)
        # Archive the Integration Jobs Data to /pipeline/archive/ Directory
        suffix = str(datetime.now(py_tz("US/Eastern")).strftime("%Y%m%d%H%M%S"))

        blob_name = f"integration_jobs/data/output_archive/integration_jobs_{suffix}.json"
        job_data_dd_fomatted = json.dumps(transformed_data)
        container_client.upload_blob(data = job_data_dd_fomatted, name = blob_name, overwrite = True)
        # Archive and Delete the Executions File to /pipeline/archive/ Directory
        suffix2 = blob_name_executions[27:]
        blob_name = f"executions/data/input_archive/executions_{suffix2}"
        job_data_dd_fomatted = json.dumps(blob_executions)
        container_client.upload_blob(data = job_data_dd_fomatted, name = blob_name, overwrite = True)
        container_client.delete_blob(blob = blob_name_executions)

        return "Success"
    except Exception as e:
        return e

# Function to Load the Data Pipeline Stats into Datadog
def f_dp_stats_gathering(dp_name, dp_container, dp_directory, dp_status, dp_start_time):
    try:
        dp_end_time = str(datetime.now(py_tz("US/Eastern")))[:-6][:23]
        if len(dp_end_time) == 19: dp_end_time = dp_end_time + '.000'
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
    response = open("config.json", "r")
    content = response.read()
    content = json.loads(content)
    key_vault_url = content['key_vault_url']
    blob_connection_string, datadog_api_key, container_client = f_get_secret(key_vault_url)
    try:
        is_data = 'No data to push'
        blob_list = container_client.list_blobs(name_starts_with = blob_name_executions_wildcard)
        for blob in blob_list:
            blob_name_executions = blob.name
            if blob_name_executions.endswith(".json"):
                
                # Call the Function to Transform the Extracted JSON Data
                try:
                    dp_start_time =  str(datetime.now(py_tz("US/Eastern")))[:-6][:23]
                    if len(dp_start_time) == 19: dp_start_time = dp_start_time + '.000'
                    blob_executions = f_read_blobs(blob_name_executions)
                    
                    blob_executions = json.dumps(blob_executions)
                    blob_executions = json.loads(blob_executions)
                    blob_executions1 = []
                    processing=["executing","dispatching","deploy_failed"]
                    for item in blob_executions["items"]:
                        status = str(item['status'])
                        if status:
                            if status not in processing:
                                blob_executions1.append(item)
                    update_data={
                        "items":blob_executions1,
                        "limit":100,
                        "offset":0,
                        "total":len(blob_executions1)
                    }
                    X=get_latest_talendblob(container_client,blob_connection_string)
                    archived_data=f_read_blobs(X)
                    json_executionsids = set(item["executionId"] for item in archived_data['items'])
                    blob_executions2=[]
                    for i in blob_executions1:
                        if i["executionId"] not in json_executionsids:
                            blob_executions2.append(i)
                    update_data={
                        "items":blob_executions2,
                        "limit":100,
                        "offset":0,
                        "total":len(blob_executions2)
                    }

                    response_f_transform = f_transform_talend_data(update_data)
                    if response_f_transform: 
                        is_data = 'Data is pushed to Datadog' 
                    

                    f_dp_stats_gathering('pl_transform_talend_integration_jobs_data','talend','integration_jobs/pipeline','Success', dp_start_time)
                except Exception as e:
                    f_dp_stats_gathering('pl_transform_talend_integration_jobs_data','talend','integration_jobs/pipeline','Failure', dp_start_time)
                # Call the Function to Load the Transformed JSON Data into Datadog
                try:
                    dp_start_time =  str(datetime.now(py_tz("US/Eastern")))[:-6][:23]
                    if len(dp_start_time) == 19: dp_start_time = dp_start_time + '.000'
                    response_f_load = f_load_talend_data(blob_name_executions, blob_executions, response_f_transform)
                    f_dp_stats_gathering('pl_load_talend_integration_jobs_data','talend','integration_jobs/pipeline','Success', dp_start_time)
                except Exception as e:
                    f_dp_stats_gathering('pl_load_talend_integration_jobs_data','talend','integration_jobs/pipeline','Failure', dp_start_time)

            return func.HttpResponse(f"{is_data}")
    except Exception as e:
  
        return func.HttpResponse(f"{e}")