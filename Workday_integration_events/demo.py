import os
import logging
import requests
from datetime import datetime
import time
from azure.storage.blob import BlobServiceClient
import json
import pytz
from pytz import timezone
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import schedule
from ..credentials import azure_keyvault_url

def get_secret():
        try:
            print("entered")
            azure_keyvault_url = "https://corpit-dev-datadg-kv-use.vault.azure.net/"
            secret_name = "WorkdayConnectionString"
            secret_name_api = "DatadogAPIKey"
        # Authenticate to Azure Key Vault using DefaultAzureCredential
            credential = DefaultAzureCredential()
            secret_client = SecretClient(vault_url=azure_keyvault_url, credential=credential)
            # Set the secret in Azure Key Vault
            connection_string = secret_client.get_secret(secret_name).value
            DD_API = secret_client.get_secret(secret_name_api).value
            print(connection_string)
            return connection_string,DD_API
            # return "yes","no"
        except Exception as e:
            return e,"fail"
            print(f"An error occurred while getting the secret in Azure Key Vault: {str(e)}")
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
        print(v_job_data_dd)
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
    api_url = f"{base_url}?Sent_Before={sent_before}&Sent_After={sent_after}&{format_param}"
    return api_url 

def extract_integration_events(cs,url):
    container_name = "workday"
    directory_path = "integration_events/data/input_archive/"
    max_sent_on_path = "integration_events/logic/param_last_extract_time.json"
    print("started")
    blob_service_client = BlobServiceClient.from_connection_string(cs)
    print(cs)
    container_client = blob_service_client.get_container_client(container_name)
    print(container_client)
    blob_list = container_client.list_blobs(name_starts_with=max_sent_on_path)
    for blob in blob_list:
        blob_client = container_client.get_blob_client(blob.name)
        if blob_client.exists():
            blob_data = blob_client.download_blob()
            content = blob_data.readall()
            if content:
                max_sent_on = json.loads(content)
    max_sent_on_time  = max_sent_on['last_extract_time']
    max_sent_on_time = datetime.strptime(max_sent_on_time, "%Y-%m-%dT%H:%M:%S.%f%z")
    current_timestamp = datetime.now().astimezone(pytz.timezone('MST')).isoformat()
    print(current_timestamp)
    api_url = generate_custom_api_url(url,current_timestamp,max_sent_on_time.isoformat())
    print(api_url)
    response = requests.get(api_url, auth=('ISU_Vertex_Integration_Events', 'Workday@Vertex!2023'))
    print(response)
    if response.status_code == 200:
        data = response.json()
        data = json.dumps(data)
        data = json.loads(data)
    # Check for duplicate "Sent_on" timestamps before saving data to the file
        for entry in data["Report_Entry"]:
            sent_on_str = entry["Sent_on"]
            if sent_on_str:
                sent_on_datetime = datetime.strptime(sent_on_str, "%Y-%m-%dT%H:%M:%S.%f%z")
                if sent_on_datetime == max_sent_on_time:
                    data["Report_Entry"].remove(entry)

        data_max_sent = data["Report_Entry"]
        if data_max_sent:
            sent = data_max_sent[0]['Sent_on']
            record={
                "last_extract_time" : data_max_sent[0]['Sent_on']
            }
            record = json.dumps(record)
            print(sent)
            print(record)
            blob_client = container_client.get_blob_client(max_sent_on_path)
            blob_client.upload_blob(record,overwrite=True)
            print("pushed")
            data = json.dumps(data)
            timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
            print(timestamp_str)
            blob_name = f"integration_events/data/input_archive/integration_event_input_{timestamp_str}.json"
            print(blob_name)
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(data,overwrite=True)
            print("Pushed the data into Blob storage")
            return 'success',data
        
        else:
        
            data = {"Report_Entry": []}
            data = json.dumps(data)
            timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
            print(timestamp_str)
            blob_name = f"integration_events/data/input_archive/integration_event_input_{timestamp_str}.json"
            print(blob_name)
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(data,overwrite=True)
            print("Pushed the data into Blob storage")
            return 'fail',data

def convert_to_est(mst_time,data):
 
  dt = datetime.strptime(mst_time, "%Y-%m-%dT%H:%M:%S.%f%z")
#   dt = dt.replace(hour=dt.hour+1)
  tz_est = pytz.timezone('US/Eastern')
  if data=='batchid':
    dt_est = dt.astimezone(tz_est).strftime("%Y%m%d%H%M%S")
    return dt_est
  else:
    dt_est = dt.astimezone(tz_est).strftime("%Y-%m-%d %H:%M:%S.%f")
    return dt_est


def convert_into_sec(time_str):
  h,m,s = time_str.split(":")
  return 3600*int(h)+60*int(m)+int(s)

def get_time(data,perc_data):
  
  if data is None:
    return None
  else:
    for i in perc_data:
      if (i['sys_name'] == data):
          return i['processing_time_90p']

def get_overshot(d1,d2,perc_data):
  proc_time = convert_into_sec(d1)
  proc_time_90p = get_time(d2,perc_data)
  if(proc_time_90p):
    return round((proc_time - proc_time_90p),3)
  else:
    return proc_time



   
def transform_integration_events(msg,data,perc_data,source,hostname,service,env,cs):
    print("Integration events")
    if msg =='success':
        print("in success")
        data = json.loads(data)
        key = ['Event_Type', 'Integration_Event', 'Response_Message', 'by_Person', 'Sent_on', 
        'Processing_Time', 'Integration_Event_Status', 'Created_From_Trigger', 'Integration_Cloud_ID', 
        'Integration_System','Current_Date_and_Time','Number_of_Errors','Number_of_Warnings','Error_Messages',
        'Warning_Messages'
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
                "workday" : {
                "custom" : {
                    "integration_event" :{
                    "header" : {
                        'env' : env,
                        'batch_id' :  int(convert_to_est(item['Current_Date_and_Time'],"batchid")),
                        # "timestamp" : item['Current_Date_and_Time'].replace('T'," ")
                        "timestamp" : convert_to_est(item['Current_Date_and_Time'],"notbatch")
                    },
                    "created_from_trigger" : item['Created_From_Trigger'],
                    "type": item['Event_Type'],
                    "name": item['Integration_Event'],
                    "response_message": item['Response_Message'],
                    "by_person" : item['by_Person'],
                    "sent_on": convert_to_est(item['Sent_on'],"notbatch"),
                    "processing_time": convert_into_sec(item['Processing_Time']),
                    "status": item['Integration_Event_Status'],
                    "cloud_id": item['Integration_Cloud_ID'],
                    "system_name": item['Integration_System'],
                    "processing_time_90p" : get_time(item['Integration_System'],perc_data),
                    "overshot_by" : get_overshot(item['Processing_Time'],item['Integration_System'],perc_data),
                    "error":{
                        "count" : item['Number_of_Errors'],
                        "message" : item['Error_Messages']
                    },
                    "warning":{
                        "count" : item['Number_of_Warnings'],
                        "message" : item['Warning_Messages'],
                    },
                    
                    }
                }
                }
            }
            updated_data.append(record)

        data_new = updated_data
        data_new1 = json.dumps(data_new,indent=4)
        with open('master.json','w') as file:
            json.dump(data_new,file,indent=4)
        timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
        blob_service_client = BlobServiceClient.from_connection_string(cs)
        container_client = blob_service_client.get_container_client('workday')
        blob_client = container_client.get_blob_client('integration_events/data/output_archive/')
        blob_name = f"integration_events/data/output_archive/integration_event_output_{timestamp_str}.json"
        print(blob_name)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(data_new1,overwrite=True)
        print("Pushed the data into Blob output storage")
        return "success",data_new
        # return data_new
    else:
        print("here")
        return 'fail',"nodata"


   
def perc_data_90(cs,container_name):
    blob_service_client = BlobServiceClient.from_connection_string(cs)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client('processing_time/data/processing_time.json')
    if blob_client.exists():
                blob_data = blob_client.download_blob()
                content = blob_data.readall()
                if content:
                    perc_data = json.loads(content)
                    return perc_data
def load_integration_events(updated_data,api,dd_url):
      url = dd_url
      headers = {
        'Accept': 'application/json',
        'DD-API-KEY': api,
        'Content-Type': 'application/json'
        }
      
      response = requests.post(url, headers=headers, json=updated_data)

      print(response) 
      return api
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

def main():
        # try:
        logging.info('Python HTTP trigger function processed a request.')
        cs,api= get_secret()
        print(cs)
        config_values= get_config_values(cs,'workday', 'integration_events/logic/param_config.json')
        try:
            v_timestamp = datetime.now(timezone("US/Eastern"))
            v_job_start_time = v_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            perc_data = perc_data_90(cs,'workday')
            msg,data = extract_integration_events(cs,config_values['workday_extract_url'])
            print(msg)
            v_result = f_job_stats_gathering("pl_extract_workday_integration_events_data", "workday", "integration_events/pipeline", "Success", v_job_start_time,cs,api)
            print(v_result)
            
        except Exception as e:
            v_result1 = f_job_stats_gathering("pl_extract_workday_integration_events_data", "workday", "integration_events/pipeline", "Failure", v_job_start_time,cs,api)
            print("cannot extract data")
        try:
            v_timestamp1 = datetime.now(timezone("US/Eastern"))
            v_job_start_time1 = v_timestamp1.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            updated_data_msg,updated_data = transform_integration_events(msg,data,perc_data,config_values['ddsource'],config_values['hostname'],config_values['service'],config_values['env'],cs)
            v_result2 = f_job_stats_gathering("pl_transform_workday_integration_events_data", "workday", "integration_events/pipeline", "Success", v_job_start_time1,cs,api)

        except Exception as e:
            
            v_result3 = f_job_stats_gathering("pl_transform_workday_integration_events_data", "workday", "integration_events/pipeline", "Failure", v_job_start_time1,cs,api)

            print("cannot transform data") 
        try:
            v_timestamp2 = datetime.now(timezone("US/Eastern"))
            v_job_start_time2 = v_timestamp2.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            update_result = load_integration_events(updated_data,api,config_values['url'])
            v_result4 = f_job_stats_gathering("pl_load_workday_integration_events_data", "workday", "integration_events/pipeline", "Success", v_job_start_time2,cs,api)

        except Exception as e:
            v_result5 = f_job_stats_gathering("pl_load_workday_integration_events_data", "workday", "integration_events/pipeline", "Failure", v_job_start_time2,cs,api)

            print("no data")
        # return func.HttpResponse(f"{v_result4} Data pushed to datadog")
        # return "success"

    # except Exception as e:
    #     return "failure"
         
        #  return func.HttpResponse(f"Data not pushed to datadog")



# def main(req: func.HttpRequest) -> func.HttpResponse:
if __name__ == "__main__":  

    print(azure_keyvault_url)
    # print("started")
    # num_iterations = 2880
    # for i in range(num_iterations):
    #         x = main()
    #         print(x)
    #         print("Iam done")
    #         time.sleep(900)
    # schedule.every().day.at("16:15").do(main)
    # schedule.every(15).minutes.do(main)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)








