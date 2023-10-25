#######################################################################################################################
## File Name: py_process_workday_user_sign_on_data.py                                                                 #
## Description: Python Script for extract,transform and load of workday user signon data.                             #
##                                                                                                                    #
## Creation Date: 2023-09-26                                                                                          #
## Created By: TEKsystems Datadog Development Team                                                                    #
## Last Modified Date: 2023-09-26                                                                                     #
## Last Modified By: TEKsystems Datadog Development Team                                                              #
#######################################################################################################################

import logging
import os
import requests
from datetime import datetime
import time
from pytz import timezone
from azure.storage.blob import BlobServiceClient
import json
import pytz
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import azure.functions as func

def get_secret_in_keyvault(key_vault_url):
    try:
        secret_name = "WorkdayConnectionString"
        secret_name_api = "DatadogAPIKey"
        secret_name_password = "WorkdayPassword"
        # Authenticate to Azure Key Vault using DefaultAzureCredential
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
        # Set the secret in Azure Key Vault
        connection_string = secret_client.get_secret(secret_name).value
        api = secret_client.get_secret(secret_name_api).value
        WD_password = secret_client.get_secret(secret_name_password).value
        return connection_string,api,WD_password

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
        v_job_id = f"{p_job_name} {p_job_start_time}"
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



def generate_custom_api_url(url,sent_before, sent_after):
    base_url = url
    format_param = "format=json"
    api_url = f"{base_url}?To_Moment={sent_before}&From_Moment={sent_after}&{format_param}"
    return api_url 

def get_config_values(kv,container_name, blob_name):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(kv)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        if blob_client.exists():
            json_data = blob_client.download_blob()
            json_content = json_data.readall()
            config_data = json.loads(json_content)
        return config_data
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

def extract_user_signon(cs,url,username,password):
    container_name = "workday"
    directory_path = "user_sign_on/data/input_archive/"
    max_sent_on_path = "user_sign_on/logic/param_last_extract_time.json"
    blob_service_client = BlobServiceClient.from_connection_string(cs)
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs(name_starts_with=max_sent_on_path)
    for blob in blob_list:
        blob_client = container_client.get_blob_client(blob.name)
        if blob_client.exists():
            blob_data = blob_client.download_blob()
            content = blob_data.readall()
            if content:
                max_sent_on = json.loads(content)
    max_sent_on_time  = max_sent_on['last_extract_time']
    max_sent_on_time = datetime.strptime(max_sent_on_time, "%Y-%m-%dT%H:%M:%S%z")
    current_timestamp = datetime.now().astimezone(pytz.timezone('MST')).isoformat()
    api_url = generate_custom_api_url(url,current_timestamp,max_sent_on_time.isoformat())
    response = requests.get(api_url, auth=(username,password))
    if response.status_code == 200:
        data = response.json()
        data = json.dumps(data)
        data = json.loads(data)
    # Check for duplicate "Sent_on" timestamps before saving data to the file
        for entry in data["Report_Entry"]:
            sent_on_str = entry["Session_Start"]
            if sent_on_str:
                sent_on_datetime = datetime.strptime(sent_on_str, "%Y-%m-%dT%H:%M:%S%z")
                if sent_on_datetime == max_sent_on_time:
                    data["Report_Entry"].remove(entry)

        data_max_sent = data["Report_Entry"]
        if data_max_sent:
            sent = data_max_sent[0]['Session_Start']
            record={
                "last_extract_time" : data_max_sent[0]['Session_Start']
            }
            record = json.dumps(record)
            blob_client = container_client.get_blob_client(max_sent_on_path)
            blob_client.upload_blob(record,overwrite=True)
            data = json.dumps(data,indent=4)
            timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
            blob_name = f"user_sign_on/data/input_archive/user_signon_input_{timestamp_str}.json"
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(data,overwrite=True)
            print("Pushed the data into Blob storage")
            return 'success',data
        
        else:
        
            data = {"Report_Entry": []}
            data = json.dumps(data)
            timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
            blob_name = f"user_sign_on/data/input_archive/user_signon_input_{timestamp_str}.json"
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(data,overwrite=True)
            print("Pushed the data into Blob storage")
            return 'fail',data

def get_diff(end_time,start_time):
    
    if end_time:
        end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S%z")
        start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S%z")
        return (end_time - start_time).total_seconds()     
    else:
        # end = str(datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
        end_time = datetime.now().astimezone(pytz.timezone('MST')).replace(microsecond=0)
        start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S%z") 
        return (end_time - start_time).total_seconds()





def convert_to_est(mst_time):
    if mst_time == '':
        return ''
    else:
        dt = datetime.strptime(mst_time, "%Y-%m-%dT%H:%M:%S%z")
        tz_est = pytz.timezone('US/Eastern')
        dt_est = str(dt.astimezone(tz_est))[:-6][:23]
        if len(dt_est) == 19: dt_est = dt_est + '.000'
        return dt_est

def convert_to_est1(mst_time,data):
    if mst_time == '':
        return ''
    else:
        dt = datetime.strptime(mst_time, "%Y-%m-%dT%H:%M:%S.%f%z")
        tz_est = pytz.timezone('US/Eastern')
        if data=='timestamp':
            dt_est = str(dt.astimezone(tz_est))[:-6][:23]
            if len(dt_est) == 19: dt_est = dt_est + '.000'
            return dt_est
        else:
            dt_est = dt.astimezone(tz_est).strftime("%Y%m%d%H%M%S")
            return dt_est


def transform_user_signon(msg,data,source,hostname,service,env,cs):

    if msg =='success':
        data = json.loads(data)
        key = ["Active_Session","Authentication Failure Message","Authentication_Type_for_Signon","Browser_Type",
        "Client_TLS_Version","Device_Type","Device_is_Trusted","Failed_Signon","ID",
        "Location_of_Signon_Worker","Operating_System","Session_End","Session_IP_Address","Session_Start","Signon",
        "Signon_Worker","Supervisory_Organization_of_Signon_Worker","System_Account","Workday_Account","Current_Date_and_Time"
        ]
        for item in data['Report_Entry']:
            for i in key:
                if i not in item:
                    item[i] = ''
        updated_data = []
          
        for item in data['Report_Entry']:

            record={
                'ddsource' : source,
                'hostname' : hostname,
                'service' : service,
                "network":{
                    "client":{
                        "ip": item['Session_IP_Address']
                    }
                },
                "workday" : {
                "custom" : {
                    "user_signon" :{
                    "header" : {
                        'env' : env,
                        'batch_id' : int(convert_to_est1(item['Current_Effective_Date_and_Time'],"batchid")),
                        "timestamp" : convert_to_est1(item['Current_Effective_Date_and_Time'],"timestamp")
                    },
                    "session":{
                        "is_active": item['Active_Session'],
                        "end_time": convert_to_est(item['Session_End']),
                        "ip_address": item['Session_IP_Address'],
                        "start_time" : convert_to_est(item['Session_Start']),
                        "duration" : get_diff(item['Session_End'],item['Session_Start'])
                    },
                    "auth" : {
                        "failure_message": item['Authentication Failure Message'],
                        "type": item['Authentication_Type_for_Signon']
                    },
                    "worker" :{
                        "location":item['Location_of_Signon_Worker'],
                        "name" : item['Signon_Worker'],
                        "supervisor" : item['Supervisory_Organization_of_Signon_Worker'],
                        "system_account" : item['System_Account'],
                        "workday_account" : item['Workday_Account']
                    },
                    "device":{
                            "type" : item['Device_Type'],
                            "is_trusted" : item['Device_is_Trusted']

                    },
                    "browser_type" : item['Browser_Type'],
                    "client_tls_version": item['Client_TLS_Version'],
                    "is_failed": item['Failed_Signon'],
                    "id": item['ID'],
                    "os" : item['Operating_System'],
                    "signon": item['Signon'] 
                    }
                }
                }
            }
            updated_data.append(record)

        data_new = updated_data
        data_new1 = json.dumps(data_new,indent=4)
        timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
        blob_service_client = BlobServiceClient.from_connection_string(cs)
        container_client = blob_service_client.get_container_client('workday')
        blob_client = container_client.get_blob_client('user_sign_on/data/output_archive/')
        blob_name = f"user_sign_on/data/output_archive/user_signon_output_{timestamp_str}.json"
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(data_new1,overwrite=True)
        print("Pushed the data into Blob output storage")
        return "success",data_new
    else:
        return 'fail',"nodata"

def load_user_signon(updated_data,api,dd_url):
      url = dd_url
      headers = {
        'Accept': 'application/json',
        'DD-API-KEY': api,
        'Content-Type': 'application/json'
        }
      
      response = requests.post(url, headers=headers, json=updated_data)
      print(response)

      return "success"


def main(req: func.HttpRequest) -> func.HttpResponse:

        logging.info('Python HTTP trigger function processed a request.')
        response = open("config.json", "r")
        content = response.read()
        content = json.loads(content)
        key_vault_url = content['key_vault_url']
        cs,api,password = get_secret_in_keyvault(key_vault_url)
        config_values= get_config_values(cs,'workday', 'user_sign_on/logic/param_config.json')
        try:
            v_timestamp = datetime.now(timezone("US/Eastern"))
            v_job_start_time = str(v_timestamp)[:-6][:23]
            if len(v_job_start_time) == 19: v_job_start_time = v_job_start_time + '.000'
            user_extract_msg,user_extract_data = extract_user_signon(cs,config_values['workday_user_url'],config_values['username'],password)
            v_result = f_job_stats_gathering("pl_extract_workday_user_sign_on_data", "workday", "user_sign_on/pipeline", "Success", v_job_start_time,cs,api)
            if user_extract_msg == 'success':
                try:
                    v_timestamp1 = datetime.now(timezone("US/Eastern"))
                    v_job_start_time1 = str(v_timestamp1)[:-6][:23]
                    if len(v_job_start_time1) == 19: v_job_start_time1 = v_job_start_time1 + '.000'
                    user_transform_msg,user_transform_data = transform_user_signon(user_extract_msg,user_extract_data,config_values['ddsource'],config_values['hostname'],config_values['service'],config_values['env'],cs)
                    print(user_transform_msg)
                    v_result2 = f_job_stats_gathering("pl_transform_workday_user_sign_on_data", "workday", "user_sign_on/pipeline", "Success", v_job_start_time1,cs,api)
                    if user_transform_msg == 'success':
                        try:
                            v_timestamp2 = datetime.now(timezone("US/Eastern"))
                            v_job_start_time2= str(v_timestamp2)[:-6][:23]
                            if len(v_job_start_time2) == 19: v_job_start_time2 = v_job_start_time2 + '.000'
                            user_result = load_user_signon(user_transform_data,api,config_values['datadog_url'])
                            v_result4 = f_job_stats_gathering("pl_load_workday_user_sign_on_data", "workday", "user_sign_on/pipeline", "Success", v_job_start_time2,cs,api)
                            print(user_result)
                        except Exception as e:
                            v_result1 = f_job_stats_gathering("pl_extract_workday_user_sign_on_data", "workday", "user_sign_on/pipeline", "Failure", v_job_start_time,cs,api)
                            return e
                except Exception as e1:
                    v_result3 = f_job_stats_gathering("pl_transform_workday_user_sign_on_data", "workday", "user_sign_on/pipeline", "Failure", v_job_start_time1,cs,api)
        except:
            v_result5 = f_job_stats_gathering("pl_load_workday_user_sign_on_data", "workday", "user_sign_on/pipeline", "Failure", v_job_start_time2,cs,api)

        return func.HttpResponse(f"Data pushed to datadog")



