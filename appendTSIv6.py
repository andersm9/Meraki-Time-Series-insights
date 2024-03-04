import requests
from http.server import HTTPServer, CGIHTTPRequestHandler
import google.auth
import google.auth.transport.requests
from google.oauth2 import service_account
import requests
import json
import meraki
import farmhash
import time
from pprint import pprint
import gcpUpload as gcpUpload
import time
import datetime
import config
import queryTSI


gcpJson = open(config.GCP_CredentialFile)        
gcpData = json.load(gcpJson)
project_id = gcpData["project_id"]

aWeek = 604800
oneWeek = 604800
oneWeekAgo = oneWeek
twoWeekAgo = oneWeek*2
API_KEY = config.KEY

#Open the tempSensor file:
with open("tempSensors.py") as t:
    tempSensors = t.read().splitlines() 

#Meraki Dashboard API call:
def dataCalls():
    dashboard = meraki.DashboardAPI(API_KEY, output_log=False)
    now = time.time()
    organization_id = '265528'

    while True:
        print("in timer loop")
        response = dashboard.sensor.getOrganizationSensorReadingsLatest(organization_id, total_pages='all')

        for entry in response:

            if entry['serial'] in tempSensors:

                count=0
                for n in entry['readings']:

                    if ('temperature' in entry['readings'][count]):

                        reading = '[{'
                        reading = '{'
                        groupID = str(farmhash.hash64(str(entry)))
                        if len(groupID) == 20:
                            groupID = groupID[:-1]
                        if len(groupID) == 19:
                            groupID = groupID[:-1]

                        now = datetime.datetime.now()
                        oldHour = now.hour
                        newHour = oldHour
                        now = now.replace(hour = newHour)
                        
                        now = str(now)
                        now = now[:-7]
                        now = now.replace(" ", "T")

                        now = now + "+00:00"

                        reading += '"eventTime":"'+now+'",'

                        reading += '"dimensions":[{'
                        reading += '"name":"serial","stringVal":"' +entry['serial']+'"},'
                        temp = str(entry['readings'][count]['temperature']['celsius'])
                        reading += '{"name":"temperature","doubleVal":' + temp +'}'
                        reading += ']' 
                        reading += '}'
                        reading += '}'
                        payload = '{"events": '
                        final = payload + reading

                        print(f"appending temperature reading ({temp}) to TSI Dataset {entry['serial'] }")
                        finalCR = final + "\n"

                        output = open("out.json", "a")
                        output.write(finalCR)
                        output.close()

                        print("output.json written")
                        credentials = service_account.Credentials.from_service_account_file(config.GCP_CredentialFile, scopes=['https://www.googleapis.com/auth/cloud-platform'])
                        auth_req = google.auth.transport.requests.Request()
                        credentials.refresh(auth_req)
                        credentials.token

                        url = f"https://timeseriesinsights.googleapis.com/v1/projects/{project_id}/datasets/{entry['serial']}:appendEvents"

                        headers = {
                            "Accept": "application/json",
                            "Authorization": f"Bearer {credentials.token}"
                        }

                        response = requests.request('POST', url, headers=headers, data = final)
                        print("---------TSI Append response code ------------:")
                        print(response)
                        queryTSI.ask(entry['serial'])
                        
                    count=count+1
        time.sleep(30)
if __name__ == '__main__':
    while True:
        try:
            dataCalls()
        except Exception as e:
            print("------exception------")
            print(e)
            output = open("out,json", "a")
            output.write(str(e))
            output.close()
        continue