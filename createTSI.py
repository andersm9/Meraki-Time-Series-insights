import requests
import json
import os
import google.auth
import google.auth.transport.requests
from google.oauth2 import service_account
import requests
import time 
import config

credentials = service_account.Credentials.from_service_account_file('friendly-hangar-300513-ab505264c787.json', scopes=['https://www.googleapis.com/auth/cloud-platform'])
auth_req = google.auth.transport.requests.Request()
credentials.refresh(auth_req)
credentials.token

gcpJson = open(config.GCP_CredentialFile)        
gcpData = json.load(gcpJson)
project_id = gcpData["project_id"]

def upload():

    url = f"https://timeseriesinsights.googleapis.com/v1/projects/{project_id}/datasets"

    with open("tempSensors.py") as t:
        tempSensors = t.read().splitlines()

    for sensor in tempSensors:
        payload_0 = '''{'''
        payload_1 = '''
         "name": '''
    
        payload_2 = f'''"{sensor}",'''

        payload_3 = '''
          "dataNames": [
          "serial",
          "temperature",
          ],
          "dataSources": [{
          '''
      
        payload_3B = f'''"uri":"gs://{config.GCP_storageProject}/{config.GCP_storageProjectFile}/{sensor}.json"
          '''
        payload_4 = '}]}'
        payload = payload_0 + payload_1 + payload_2 + payload_3 + payload_3B + payload_4
        headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {credentials.token}"
        }

        response = requests.request('POST', url, headers=headers, data = payload)
        print(f'upload status = {response.status_code}')

def check_status():
    print("nothing")

if __name__ == "__main__":
    upload()
    ###Get the status of uploaded datasets in creating timeseries:
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {credentials.token}"
        }

    url = f'https://timeseriesinsights.googleapis.com/v1/projects/{project_id}/datasets'
    upload_status = requests.request('GET', url, headers=headers)
    upload_status = upload_status.json()
    GlobalTS_List = upload_status['datasets']
    loadedTS = 0
    OperatingTS = 0
    FailedTS = 0

    ###Check number of TS we are expecting
    with open("tempSensors.py") as t:
        tempSensors = t.read().splitlines()
    sensorCount = len(tempSensors)
    print(f'sensorCount = {sensorCount}')
    
    ###Check the status of each TS
    while loadedTS < sensorCount:
        upload_status = requests.request('GET', url, headers=headers)
        upload_status = upload_status.json()
        GlobalTS_List = upload_status['datasets']
        loadedTS = 0
        OperatingTS = 0
        FailedTS = 0
        ###check the status of the TS uploads - LOADING,LOADED, UNLOADED or FAILED
        for timeSeries in GlobalTS_List:
            TS_Name = timeSeries['name']
            print(f"checking the state of {TS_Name} state = {timeSeries['state']}")
            ###Look for TS that match the ones wwe created

            for sensor in tempSensors:
                if TS_Name == f"projects/{project_id}/locations/{config.GCP_location}/datasets/{sensor}":
                    if timeSeries['state'] == 'LOADED':
                      print(f"{TS_Name} status = {timeSeries['state']}")
                      loadedTS += 1
                      ###do nothing - but count and exit if there are 5
                      if loadedTS == len(tempSensors):
                          print("exit here")
                          exit()
                    if timeSeries['state'] ==  'LOADING'or timeSeries['state'] == 'UNLOADING':
                      print("TS is LOADING / UNLOADING")
                      print(f"{TS_Name} status = {timeSeries['state']}")
                      OperatingTS +=1
                      ###do nothing - waiting
                    if timeSeries['state'] == 'FAILED':
                      ###delte the TS
                      print(f"deleting a failed TS - {TS_Name}")
                      print(f"{TS_Name} status = {timeSeries['state']}")
                      url = f'https://timeseriesinsights.googleapis.com/v1/projects/{project_id}datasets/' + sensor  
                      FailedTS += 1
                      response = requests.delete(url,headers=headers)
                      print(f"delete status_code = {response.status_code}")
                    print(f"TS count: LOADED = {loadedTS}, LOADING/UNLOADING = {OperatingTS}, FAILED = {FailedTS}\n")
                    ###Check if there are fewer TS (in any state) than the sensor count - if so, reload
                    inFlightTS = loadedTS + OperatingTS + FailedTS
        if inFlightTS < sensorCount:
          print("Sensor is absent from TS API, reload to dataset")
          upload()
                    
    ### pause to wait for the TS API to change status before checking again
        print("sleep")
        time.sleep(30)
        print("--------------------------------------------------------------------------")                