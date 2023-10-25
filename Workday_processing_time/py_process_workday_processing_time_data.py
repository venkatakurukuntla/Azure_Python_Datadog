import logging
import os
import numpy as np
import json
import time
import requests
import pytz
from pytz import timezone
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from datetime import timedelta
import azure.functions as func



def get_secret(key_vault_url):
        try:

            secret_name = "WorkdayConnectionString"
            secret_name_api = "DatadogAPIKey"
            secret_name_password = "WorkdayPassword"
        # Authenticate to Azure Key Vault using DefaultAzureCredential
            credential = DefaultAzureCredential()
            secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
            # Set the secret in Azure Key Vault
            connection_string = secret_client.get_secret(secret_name).value
            DD_API = secret_client.get_secret(secret_name_api).value
            WD_password = secret_client.get_secret(secret_name_password).value
            return connection_string,DD_API,WD_password

        except Exception as e:
            return e,"fail"," "


def f_read_json_from_blob(p_container, p_directory, p_file_name,cs):
    try:
        
        v_blob_name = os.path.join(p_directory, p_file_name)
        v_blob_service_client = BlobServiceClient.from_connection_string(cs)
        v_container_client = v_blob_service_client.get_container_client(p_container)
        v_blob_client = v_container_client.get_blob_client(v_blob_name)
        return v_blob_client
    
    except Exception as v_exception:
        return v_exception
def f_write_json_to_blob(p_container, p_directory, p_file_name, p_json_data,cs):
    try:
        v_application= p_container.capitalize()
        v_location = p_directory
        v_blob_name = os.path.join(v_location, p_file_name)
        
        v_blob_service_client = BlobServiceClient.from_connection_string(cs)
        v_container_client = v_blob_service_client.get_container_client(p_container)
        v_container_client.upload_blob(data=p_json_data, name=v_blob_name, overwrite=True)
        return "Success"
    
    except Exception as v_exception:
        return v_exception

def f_job_stats_gathering(p_job_name, p_container, p_directory, p_job_status, p_job_start_time,cs,api):
    try:
        v_timestamp = datetime.now(timezone("US/Eastern"))
        v_job_start_time_dttm = datetime.strptime(p_job_start_time, '%Y-%m-%d %H:%M:%S.%f')
        v_job_end_time_dttm = datetime.strptime(v_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], '%Y-%m-%d %H:%M:%S.%f')
        v_dttm_diff = v_job_end_time_dttm - v_job_start_time_dttm
        v_directory = f"{p_directory}/logic/"
        
        v_config_file_name = "config.json"
        v_blob_client_config = f_read_json_from_blob(p_container, v_directory, v_config_file_name,cs)
        v_json_data_config = v_blob_client_config.download_blob()
        v_json_content_config = v_json_data_config.readall()
        v_data_config = json.loads(v_json_content_config)
        
        v_ddsource = v_data_config.get('ddsource')
        v_hostname = v_data_config.get('hostname')
        v_service = v_data_config.get('service')
        v_job_id = f"{p_job_name} {p_job_start_time}.json"
        v_job_name = p_job_name
        v_job_start_time = p_job_start_time
        v_job_end_time = v_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        v_job_duration = v_dttm_diff.total_seconds()
        p_job_status = p_job_status
        v_env = v_data_config.get('env')
        v_timestamp_num = v_timestamp.strftime("%Y%m%d%H%M%S")
        v_application = p_container.capitalize()
        v_object = p_directory.capitalize()[:-9]
        v_dd_url= v_data_config.get('dd_url')    

        v_job_data_dd = {
            "ddsource":v_ddsource,
            "hostname":v_hostname,
            "service":v_service,
            "data_pipeline":
                {"custom":
                    {"job":
                        {"id":v_job_id,
                        "name":v_job_name,
                        "start_time":v_job_start_time,
                        "end_time":v_job_end_time,
                        "duration":v_job_duration,
                        "status":p_job_status},
                    "header":
                        {"timestamp":v_job_end_time,
                        "env":v_env,
                        "batch_id":int(v_timestamp_num),
                        "application":v_application,
                        "object":v_object
                        }
                    }
                }
            }
        v_json_data_dd = json.dumps(v_job_data_dd)
        v_headers = {
            'Accept': 'application/json',
            'DD-API-KEY': api,
            'Content-Type': 'application/json'
            }
        # Push the JSON Data to Datadog
        requests.post(v_dd_url, headers=v_headers, json=v_job_data_dd)
        
        # Archive the JSON File to /pipeline/arhive/ Directory
        v_file_name_archive = f"{p_job_name}__{v_timestamp_num}.json"
        f_write_json_to_blob(p_container, f"{p_directory}/archive/", v_file_name_archive, v_json_data_dd,cs)
        
        return "Success"
    
    except Exception as v_exception:
        return v_exception



def generate_custom_api_url(sent_before, sent_after,url):
    base_url = "https://wd2-impl-services1.workday.com/ccx/service/customreport2/vertexinc4/ISU_Vertex_Integration_Events/CR_-_Integration_Events_-_Vertex"
    format_param = "format=json"
    api_url = f"{base_url}?Sent_Before={sent_before}&Sent_After={sent_after}&{format_param}"
    return api_url 

def convert_into_sec(time_str):
  if time_str == None:
    return 0
  else:
    h,m,s = time_str.split(":")
    return 3600*int(h)+60*int(m)+int(s)


def create_dict(latest_data):
    results = {}
# Iterate through the data and calculate the 90th percentile for each task ID
    for item in latest_data:
        sys_name = item['integration_system']
        proc_time = item['processing_time']

        if sys_name not in results:
            results[sys_name] = []
        results[sys_name].append(proc_time)

    return results

def create_90p(results):
    # Calculate and print the 90th percentile for each task ID
    percentile_results=[]
    for task_id, durations in results.items():
        percentile_90 = round(np.percentile(durations, 90),3)
        result={"sys_name":task_id,"processing_time_90p":percentile_90}
        percentile_results.append(result)
    return percentile_results

def get_config_values(cs,container_name, blob_name):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(cs)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        if blob_client.exists():
            json_data = blob_client.download_blob()
            json_content = json_data.readall()
            config_data = json.loads(json_content)
        return config_data
    except Exception as e:
        return ''


def processing_time_extract(cs,url,username,password):
    container_name = "workday"
    directory_path = "processing_time/data/output_archive"
    blob_service_client = BlobServiceClient.from_connection_string(cs)
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs(name_starts_with=directory_path)
    current_timestamp = datetime.now().isoformat()
    today = datetime.today()
    one_month_ago = today - timedelta(days=30)
    api_url = generate_custom_api_url(current_timestamp, one_month_ago.isoformat(),url)
    response = requests.get(api_url, auth=(username, password))
    if response.status_code == 200:
        data = response.json()
        data = json.dumps(data)
        data1 = json.loads(data)
    return data1

def processing_time_transform(data,cs):
        key = ['Integration_System', 'Processing_Time']
        for item in data['Report_Entry']:
            for i in key:
                if i not in item:
                    item[i] = None
        latest_data = data['Report_Entry']
        updated_data=[]
        for i in latest_data:
            record={
                'integration_system' : i['Integration_System'],
                'processing_time' : convert_into_sec(i['Processing_Time'])
            }
            updated_data.append(record)
        results = create_dict(updated_data)
        perc_results = create_90p(results) 
        percentile_results = json.dumps(perc_results)
        timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
        blob_name = f"processing_time/data/output_archive/processing_time_{timestamp_str}.json"
        blob_service_client = BlobServiceClient.from_connection_string(cs)
        container_client = blob_service_client.get_container_client('workday')
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(percentile_results,overwrite=True)
        print("Pushed the data into output_archive Blob storage")
        blob_name = f"processing_time/data/processing_time.json"
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(percentile_results,overwrite=True)
        print("updated the data with latest value")
        return percentile_results


def main(req: func.HttpRequest) -> func.HttpResponse:
    try :
        logging.info('Python HTTP trigger function processed a request.')
        response = open("config.json", "r")
        content = response.read()
        content = json.loads(content)
        key_vault_url = content['key_vault_url']
        cs,api,password = get_secret(key_vault_url)
        try:
            v_timestamp = datetime.now(timezone("US/Eastern"))
            v_job_start_time = str(v_timestamp)[:-6][:23]
            if len(v_job_start_time) == 19: v_job_start_time = v_job_start_time + '.000'
            config_values= get_config_values(cs,'workday', 'integration_events/logic/param_config.json')

            v_result = f_job_stats_gathering("pl_transform_workday_processing_time_data", "workday", "processing_time/pipeline", "Success", v_job_start_time,cs,api)


        except Exception as e:

            v_result1 = f_job_stats_gathering("pl_transform_workday_processing_time_data", "workday", "processing_time/pipeline", "Failure", v_job_start_time,cs,api)
            return e
        try:
            v_timestamp1 = datetime.now(timezone("US/Eastern"))
            v_job_start_time1 = str(v_timestamp1)[:-6][:23]
            if len(v_job_start_time1) == 19: v_job_start_time1 = v_job_start_time1 + '.000'
            process_extract = processing_time_extract(cs,config_values['workday_extract_url'],config_values['username'],password)
            process_transform = processing_time_transform(process_extract,cs)
            v_result2 = f_job_stats_gathering("pl_load_workday_processing_time_data", "workday", "processing_time/pipeline", "Success", v_job_start_time1,cs,api)
        except Exception as e:

            v_result3 = f_job_stats_gathering("pl_load_workday_processing_time_data", "workday", "processing_time/pipeline", "Failure", v_job_start_time,cs,api)
            return e,"after transform"
        return func.HttpResponse(f"{v_result2}")
    except Exception as e:
        return func.HttpResponse(f"{e} Error Occured while processing the time  ")




 
