#######################################################################################################################
## File Name: py_process_salesforce_contact_data.py                                                                 #
## Description: Python Script for transform and load of salesforce contact object.                                         #
##                                                                                                                    #
## Creation Date: 2023-09-26                                                                                          #
## Created By: TEKsystems Datadog Development Team                                                                    #
## Last Modified Date: 2023-09-26                                                                                     #
## Last Modified By: TEKsystems Datadog Development Team                                                              #
#######################################################################################################################
import io
import os
import csv
import json
import math
import logging
import requests
import pandas as pd
from pytz import timezone
from datetime import datetime
import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

def get_secret(key_vault_url):
        try:
            secret_name = "SalesforceConnectionString"
            secret_name_API="DatadogAPIKey"
            # Authenticate to Azure Key Vault using DefaultAzureCredential
            credential = DefaultAzureCredential()
            secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
            # Set the secret in Azure Key Vault
            connection_string = secret_client.get_secret(secret_name).value
            API_KEY = secret_client.get_secret(secret_name_API).value
            return connection_string,API_KEY
        except Exception as e:
            print(f"An error occurred while getting the secret in Azure Key Vault: {str(e)}")

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
        print(f"An error occurred: {str(e)}")
        return None

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
        x=requests.post(v_dd_url, headers=v_headers, json=v_job_data_dd)
        print("response",x)
        # Archive the JSON File to /pipeline/arhive/ Directory
        v_file_name_archive = f"{p_job_name}__{v_timestamp_num}.json"
        f_write_json_to_blob(p_container, f"{p_directory}/archive/", v_file_name_archive, v_json_data_dd,cs)
        
        return "Success"
    
    except Exception as v_exception:
        return v_exception
def get_latest_order_timestamp_blob(container_name,cs):
    blob_name1="contact/data/input_archive/contact_"
    def format_contact_number(contact_number):
                return f'{int(contact_number):08}'
    # Get a list of blobs in the container
    csv_blobs=[]
    blob_service_client = BlobServiceClient.from_connection_string(cs)
    container_client = blob_service_client.get_container_client('salesforce')
    blob_list = container_client.list_blobs(name_starts_with=blob_name1)
    for blob in blob_list:
        if blob.name.endswith(".csv"):
            csv_blobs.append(blob.name)
    
    if not csv_blobs:
        return None
    # Get the latest blob based on timestamp
    latest_blob_name = max(csv_blobs)
    return latest_blob_name

def process_failure_data(connection_string, container_name,latest_blob_name):
    container_name = "salesforce"
    blob_name1 = "contact/data/contact_"
    blob_name2 = "contact/data/contact_hist.json"
    blob_name3="contact/data/timestamp.json"
    contact_hist_json_path_in_blob = "contact/data/contact_hist.json"
    #contact_hist_json_path_in_blob2 = "contact/data/contact.json"
    try:
        msg = ''
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        csv_blobs = []
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=blob_name1)
        for blob in blob_list:
            if blob.name.endswith(".csv"):
                csv_blobs.append(blob.name)
        if not csv_blobs:
            print(f"No CSV files found in the '{blob_name1}' folder.")
            return
        for blob_name1 in csv_blobs:
            print(f"Processing CSV blob: {blob_name1}")
            def format_contact_number(contact_number):
                return f'{int(contact_number):08}'
            blob_client1 = container_client.get_blob_client(blob_name1)
            blob_client2 = blob_service_client.get_blob_client(container_name, blob_name2)
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client(container_name)
            blob_client3 = container_client.get_blob_client(blob_name3)
            if blob_client1.exists() and blob_client2.exists() and blob_client3.exists() :
                print("Both blobs exist")
                blob_data1 = blob_client1.download_blob()
                content1 = blob_data1.readall().decode('utf-8')

                blob_data2 = blob_client2.download_blob()
                content2 = blob_data2.readall().decode('utf-8')
                
                blob_data = blob_client3.download_blob()
                content = blob_data.readall()
                
                timestamp_data = json.loads(content)
                
                json_data = json.loads(content2)
                
                csv_df = pd.read_csv(io.StringIO(content1)).fillna("")
                
                data_list = csv_df.to_dict(orient='records')
                json_ordernumbers = set(item["Id"] for item in json_data)
                filtered_csv_df = csv_df[~csv_df["Id"].isin(json_ordernumbers)]
                filtered_data_list = filtered_csv_df.to_dict(orient='records')
                json_data.extend(filtered_data_list)
                
                for record in filtered_data_list:
                    record["TIMESTAMP"] = timestamp_data["timestamp"]
                
                for record in json_data:
                    record["TIMESTAMP"] = timestamp_data["timestamp"]
                    record["STATUS_CODE"] =-1

                container_client = blob_service_client.get_container_client(container_name)
                blob_client_order_json = container_client.get_blob_client(contact_hist_json_path_in_blob)
                blob_client_order_json.upload_blob(json.dumps(json_data, indent=4), overwrite=True)

                new_archive_blob_name = latest_blob_name
                print("new_archive_blob_name",new_archive_blob_name)

                new_archive_blob_client = container_client.get_blob_client(new_archive_blob_name)
                
                if new_archive_blob_client.exists():
                    new_archive_blob_client1 = new_archive_blob_client.download_blob()
                    content12 = new_archive_blob_client1.readall().decode('utf-8')
                    csv_df1 = pd.read_csv(io.StringIO(content12)).fillna("")
                    csv_df1['Id'] = csv_df1['Id']
                    are_equal = csv_df['Id'].equals(csv_df1['Id'])

                    if are_equal:
                        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                        container_client = blob_service_client.get_container_client(container_name)
                        container_client.delete_blob(blob_name1)
                        msg = 'Done'

                return "success",filtered_data_list,msg

            else:
                return "Error: One or both blobs do not exist","",""

    except Exception as e:
        return f"An error occurred: {str(e)}"

def fix_processing(connection_string, container_name):
    container_name = "salesforce"
    blob_name1 = "contact/data/contact_"
    blob_name2 = "contact/data/contact_hist.json"
    blob_name3 = "contact/data/contact_fix_hist.json"
    blob_name4="contact/data/timestamp.json"
    contact_hist_json_path_in_blob2 = "contact/data/contact_fix_hist.json"

    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        csv_blobs = []
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=blob_name1)
        for blob in blob_list:
            if blob.name.endswith(".csv"):
                csv_blobs.append(blob.name)
        if not csv_blobs:
            print(f"No CSV files found in the '{blob_name1}' folder.")
            return

        for blob_name1 in csv_blobs:
            print(f"Processing CSV blob: {blob_name1}")
            def format_contact_number(contact_number):
                return f'{int(contact_number):08}'
            blob_client1 = container_client.get_blob_client(blob_name1)
            blob_client2 = blob_service_client.get_blob_client(container_name, blob_name2)
            blob_client3 = blob_service_client.get_blob_client(container_name, blob_name3)
            blob_client4= blob_service_client.get_blob_client(container_name, blob_name4)
            if blob_client1.exists() and blob_client2.exists() and blob_client3.exists():
                print("All three blobs exist")
                blob_data1 = blob_client1.download_blob()
                content1 = blob_data1.readall().decode('utf-8')

                blob_data2 = blob_client2.download_blob()
                content2 = blob_data2.readall().decode('utf-8')

                blob_data3 = blob_client3.download_blob()
                content3 = blob_data3.readall()

                blob_data4 = blob_client4.download_blob()
                content4 = blob_data4.readall()

                json_data2 = json.loads(content2)
                json_data3 = json.loads(content3)
                timestamp_data = json.loads(content4)

                csv_df = pd.read_csv(io.StringIO(content1))
                datalist = csv_df.to_dict(orient='records')
                
                json_ordernumbers2 = set(item["Id"] for item in json_data2)
                filtered_csv_df = csv_df[~csv_df["Id"].isin(json_ordernumbers2)]
                filtered_json_data2 = [item for item in json_data2 if item["Id"] not in [record["Id"] for record in datalist]]

                json_ordernumbers3 = set(item["Id"] for item in json_data3)
                filtered_json_data3 = [item for item in filtered_json_data2 if item["Id"] not in json_ordernumbers3]
                timestamp = datetime.strptime(timestamp_data["timestamp"], "%Y-%m-%d %H:%M:%S.%f")
            
                for record in filtered_json_data2:
                    record["TIMESTAMP"] = timestamp_data["timestamp"]
                    record["STATUS_CODE"] =1
                    fix_last_modified=record.get("LastModifiedDate")
                    timestamp=record.get("TIMESTAMP")
                    if fix_last_modified and timestamp:
                        fix_last_modified1=fix_last_modified[:26]
                        fix_time = datetime.strptime(fix_last_modified1, "%Y-%m-%d %H:%M:%S.%f")
                        timestamp_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
                        time_difference = (fix_time - timestamp_time).total_seconds()
                        record["SECONDS_TO_FIX"] = time_difference
                    

                json_data3.extend(filtered_json_data3)

                container_client = blob_service_client.get_container_client(container_name)
                
                blob_client_order_json2f = container_client.get_blob_client(contact_hist_json_path_in_blob2)
                blob_client_order_json2f.upload_blob(json.dumps(json_data3, indent=4), overwrite=True)
                return "success",filtered_json_data3
            else:
                return "Error: One or more blobs do not exist"
    except Exception as e:
        return f"An error occurred: {str(e)}"

def contact(json_data,get_configvalues,cs,msg):
  if msg =='success':
    updated_data = []
    for item in json_data:
      batch_id_timestamp=item['TIMESTAMP']
      batch_datetime = datetime.strptime(batch_id_timestamp, "%Y-%m-%d %H:%M:%S.%f")
      batchid=batch_datetime.strftime("%Y%m%d%H%M%S")
      record={
          'ddsource' : get_configvalues["ddsource"],
          'hostname' : "",
          'service' : get_configvalues["service"],
          "salesforce" : {
            "custom" : {
              "contact" :{
                "header" : {
                  'env' : get_configvalues["env"],
                  'batch_id' : int(batchid),
                  "date_id" : math.trunc(float(batchid)/1000000),
                  "timestamp":item['TIMESTAMP'],
                },
                "id" : item['Id'],
                "salutation": item['Salutation'],
                "first_name": item['FirstName'],
                "last_name": item['LastName'],
                "title" : item['Title'],
                "phone": item['Phone'],
                "mobile": item['MobilePhone'],
                "fax": item['Fax'],
                "email": item['Email'],
                "sync_to_profisee": item['Sync_to_Profisee__c'],
                "lastmodifieddate":item['LastModifiedDate'][:23],
                "status_code":item['STATUS_CODE'],
                "seconds_to_fix": "",
                "mailing":{
                  "street" : item['MailingStreet'],
                  "city" : item['MailingCity'],
                  "state" : item['MailingState'],
                  "postal_code" : item['MailingPostalCode'],
                  "country" : item['MailingCountry'],
                },
                "integration":{
                  "status" : item['Integration_Status__c'],
                  "error_message" : item['Integration_Error_Message__c'],
                },
                "Account":{
                  "owner" : item['Account.Owner.Name'],
                  "name" : item['Account.Name'],
                },
                "profisee":{
                  "external_id" : item['Profisee_External_ID__c'],
                  "master_id" : item['Profisee_Master_Id__c'],
                }
              }
            }
          }
        }
 
      updated_data.append(record)
    data_new = updated_data
    data_new1 = json.dumps(data_new,indent=4)
    timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
    blob_service_client = BlobServiceClient.from_connection_string(cs)
    container_client = blob_service_client.get_container_client('salesforce')
    blob_client = container_client.get_blob_client('contact/data/output_archive/')
    blob_name = f"contact/data/output_archive/contact_fail_{timestamp_str}.json"
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(data_new1,overwrite=True)
    return data_new
  else:
      return 'no data'



def push_contact(get_configvalues,Api,updated_data_order):
    url = get_configvalues["dd_url"]
    headers = {
    'Accept': 'application/json',
    'DD-API-KEY': Api,
    'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, json=updated_data_order)
    print(response)

def contactfix(filtered_json_data3,get_configvalues,cs,msg):
  if msg =='success':
    updated_data = []
    for item in filtered_json_data3:
      batch_id_timestamp=item['TIMESTAMP']
      batch_datetime = datetime.strptime(batch_id_timestamp, "%Y-%m-%d %H:%M:%S.%f")
      batchid=batch_datetime.strftime("%Y%m%d%H%M%S")
      record={
          'ddsource' : get_configvalues["ddsource"],
          'hostname' : "",
          'service' : get_configvalues["service"],
          "salesforce" : {
            "custom" : {
              "contact" :{
                "header" : {
                  'env' : get_configvalues["env"],
                  'batch_id' : int(batchid),
                  "date_id" : math.trunc(float(batchid)/1000000),
                  "timestamp":item['TIMESTAMP'],
                },
                "id" : item['Id'],
                "salutation": item['Salutation'],
                "first_name": item['FirstName'],
                "last_name": item['LastName'],
                "title" : item['Title'],
                "phone": item['Phone'],
                "mobile": item['MobilePhone'],
                "fax": item['Fax'],
                "email": item['Email'],
                "sync_to_profisee": item['Sync_to_Profisee__c'],
                "lastmodifieddate":item['LastModifiedDate'][:23],
                "status_code":item['STATUS_CODE'],
                "seconds_to_fix":item['SECONDS_TO_FIX'],
                "mailing":{
                  "street" : item['MailingStreet'],
                  "city" : item['MailingCity'],
                  "state" : item['MailingState'],
                  "postal_code" : item['MailingPostalCode'],
                  "country" : item['MailingCountry'],
                },
                "integration":{
                  "status" : item['Integration_Status__c'],
                  "error_message" : item['Integration_Error_Message__c'],
                },
                "Account":{
                  "owner" : item['Account.Owner.Name'],
                  "name" : item['Account.Name'],
                },
                "profisee":{
                  "external_id" : item['Profisee_External_ID__c'],
                  "master_id" : item['Profisee_Master_Id__c'],
                }
              }
            }
          }
        }
      
      updated_data.append(record)
    data_new = updated_data
    data_new1 = json.dumps(data_new,indent=4)
    timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
    blob_service_client = BlobServiceClient.from_connection_string(cs)
    container_client = blob_service_client.get_container_client('salesforce')
    blob_client = container_client.get_blob_client('contact/data/output_archive/')
    blob_name = f"contact/data/output_archive/contact_fix_{timestamp_str}.json"
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(data_new1,overwrite=True)
    return data_new
  else:
      return "no data"

def push_contact_fix(get_configvalues,Api,contact_fix):
    url = get_configvalues["dd_url"]
    headers = {
    'Accept': 'application/json',
    'DD-API-KEY': Api,
    'Content-Type': 'application/json'
    }

    response = requests.post(url, headers=headers, json=contact_fix)
    print(response)

def moving_files(cs):
    
    container_name = "salesforce"
    source_folder = "contact/data/"
    output_archive_folder = "contact/data/output_archive/"
    source_folder1 = "contact/data/contact_"
    input_archive_folder= "contact/data/input_archive/contact_"
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    blob_service_client = BlobServiceClient.from_connection_string(cs)
    container_client = blob_service_client.get_container_client(container_name)

    blob_list = container_client.list_blobs(name_starts_with=source_folder1)
    for blob in blob_list:
        if blob.name.lower().endswith('.csv'):
            source_blob_client = container_client.get_blob_client(blob)
            destination_blob_name = blob.name.replace(source_folder1, input_archive_folder, 1)
            destination_blob_client = blob_service_client.get_blob_client(container_name, destination_blob_name)
            destination_blob_client.start_copy_from_url(source_blob_client.url)
            container_client.delete_blob(blob)
    return 'Files moved successfully'
    
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    container_name = "salesforce"
    blob_name="contact/logic/config.json"
    response = open("config.json", "r")
    content = response.read()
    content = json.loads(content)
    key_vault_url = content['key_vault_url']
    cs,Api=get_secret(key_vault_url)
    latest_blob_name=get_latest_order_timestamp_blob(container_name,cs)
    print("latest_blob_name",latest_blob_name)
    get_configvalues=get_config_values(cs,container_name, blob_name)
    x,filtered_data_list,msg=process_failure_data(cs, container_name,latest_blob_name)
    print(msg)
    if msg=="Done":
        return "No new failures"
    else:
        msg,filtered_json_data3=fix_processing(cs, container_name)
        try:
                v_timestamp = datetime.now(timezone("US/Eastern"))
                v_job_start_time = str(v_timestamp)[:-6][:23]
                if len(v_job_start_time) == 19: v_job_start_time = v_job_start_time + '.000'
                updated_data_contact = contact(filtered_data_list,get_configvalues,cs,msg)
                contact_fix=contactfix(filtered_json_data3,get_configvalues,cs,msg)
                v_result2 = f_job_stats_gathering("pl_transform_salesforce_contact_data", "salesforce", "contact/pipeline", "Success", v_job_start_time,cs,Api)         
        except Exception as e:
                v_result3 = f_job_stats_gathering("pl_transform_salesforce_contact_data", "salesforce", "contact/pipeline", "Failure", v_job_start_time,cs,Api)
        
        try:
                v_timestamp1 = datetime.now(timezone("US/Eastern"))
                v_job_start_time1 = str(v_timestamp1)[:-6][:23]
                if len(v_job_start_time1) == 19: v_job_start_time1 = v_job_start_time1 + '.000'
                pushcontact=push_contact(get_configvalues,Api,updated_data_contact)
                push_contactfix=push_contact_fix(get_configvalues,Api,contact_fix)
                v_result2 = f_job_stats_gathering("pl_load_salesforce_contact_data", "salesforce", "contact/pipeline", "Success", v_job_start_time1,cs,Api)
        except Exception as e:
                v_result3 = f_job_stats_gathering("pl_load_salesforce_contact_data", "salesforce", "contact/pipeline", "Failure", v_job_start_time1,cs,Api)
                
        move_files=moving_files(cs)
        return func.HttpResponse("data pushed to datadog")