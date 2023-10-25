import requests
import json
import azure.functions as func

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Function Main
# if __name__ == "__main__":
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        response = open("config.json", "r")
        content = response.read()
        content = json.loads(content)
        key_vault_url = content['key_vault_url']
        secret_name_basic = "TalendBasicAuthorization"
        talend_api_url = "https://api.us.cloud.talend.com/security/oauth/token"
        secret_name_bearer = "TalendBearerToken"
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url = key_vault_url, credential = credential)
        talend_authorization = secret_client.get_secret(secret_name_basic).value
        payload = json.dumps({
            "audience": "https://api.us.cloud.talend.com",
            "grant_type": "client_credentials"
        })
        headers = {
        'Content-Type': 'application/json',
        'Authorization': talend_authorization
        }
        response = requests.request("POST", talend_api_url, headers = headers, data = payload)
        response_json = json.loads(response.text)
        secret_value = 'Bearer ' + response_json['access_token'] 
        if response_json['access_token']:
            secret_client.set_secret(secret_name_bearer, secret_value)
        print('Success')
        return func.HttpResponse(f"Success")
    except Exception as e:
        print(e)
        return func.HttpResponse(f"Failure: {e}")