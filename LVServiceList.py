import json
import requests

### Get session token for future API calls

session_token_endpoint = "https://api.na6.livevox.com/session/login"
session_token_headers = {
    "LV-Access": "eefe858c-430c-4788-b5a2-7d33bb0ec611",
    "Content-Type": "application/json",
}
session_token_data = {
    "clientName": "LV_NA6_0043",
    "userName": "APIUSER",
    "password": "iLending092023!",
    "agent": "false",
}

response = requests.post(
    session_token_endpoint, headers=session_token_headers, json=session_token_data
).json()

session_token = response.get("sessionId")

service_list_endpoint = "https://api.na6.livevox.com/configuration/services?callCenter=3063064&offset=0&count=250"
livevox_headers = {"LV-Session": session_token}


service_list_response = requests.get(
    service_list_endpoint,
    headers=livevox_headers,
).json()

service_list_report = service_list_response.get("service")

service_list = []

for dictionary in service_list_report:
    values = list(dictionary.values())
    service_list.append(values)

list_of_services = []

# for sublist in service_list:
for sublist in service_list:
    list_of_services.append(sublist[0])


print(list_of_services)
