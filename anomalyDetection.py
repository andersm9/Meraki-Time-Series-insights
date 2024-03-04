import json
import meraki
import farmhash
import time
from pprint import pprint
import gcpUpload as gcpUpload
import createTSI
import config
from PyInquirer import prompt
import appendTSIv6
from multiprocessing import Process

aDay = 86400
aWeek = 604800

question = [
    {
               'type': 'input',
               'name': 'days',
                'message': 'How many days of historic data should be gathered?',
    }
]

answer = prompt(question)
quantityDays = float(answer['days'])
print(f'qunatityDays = {quantityDays}')

timeSpan = quantityDays*aDay
now = time.time()
globalEndTime = now
globalStartTime = globalEndTime - timeSpan

localEndTime = globalEndTime

if timeSpan >= aWeek:
    localStartTime = now - aWeek
if timeSpan < aWeek:
    localStartTime = now - timeSpan


API_KEY = config.KEY


dashboard = meraki.DashboardAPI(API_KEY,output_log=False)
organization_id = config.ORG
GCP_CredentialFile = config.GCP_CredentialFile
GCP_storageProject = config.GCP_storageProject

tempSensors = []

while timeSpan > 0:

    response = dashboard.sensor.getOrganizationSensorReadingsHistory(
        organization_id,t0 = localStartTime,t1=localEndTime, total_pages='all',print_console=False
    )

    if timeSpan >= aWeek:
        timeSpan = timeSpan - aWeek
        localEndTime = localStartTime
        localStartTime = localStartTime - aWeek
    if timeSpan < aWeek:
        localEndTime = localStartTime
        localStartTime = localStartTime - timeSpan
        timeSpan = 0
    iterationCount =0

    print(f'timeSpan = {timeSpan}')
    print(f'localStarttime = {localStartTime}')
    print(f'localEndTime {localEndTime} \n')

    for entry in response:

        if True: 
            if entry['metric'] == 'temperature':
                newSensorFlag = False
                if entry["serial"] not in tempSensors:
                    tempSensors.append(entry["serial"])
                    print(f"found sensor {entry['serial']}")
                    newSensorFlag = True

                reading = '{'
                groupID = str(farmhash.hash64(str(entry)))
                if len(groupID) == 20:
                    groupID = groupID[:-1]
                if len(groupID) == 19:
                    groupID = groupID[:-1]
                reading += '"groupId":' +'"' + groupID + '",' 
                time = entry['ts'] 
                time = time[:-1]
                time = time + "+00:00"
                reading += '"eventTime":"'+time+'",'
                reading += '"dimensions":[{'
                reading += '"name":"serial","stringVal":"' +entry['serial']+'"},'
                temp = str(entry["temperature"]["celsius"])
                reading += '{"name":"temperature","doubleVal":' + temp +'}'
                reading += ']' 
                reading += '}'
                reading += '\n'
                Dynamic_Variable_Name = "sensorData_" + entry['serial']
                if newSensorFlag == True:
                    vars()[Dynamic_Variable_Name] = open(f'DataFiles/sensorOut_{entry["serial"]}.json','w')
                if newSensorFlag == False:
                    vars()[Dynamic_Variable_Name] = open(f'DataFiles/sensorOut_{entry["serial"]}.json','a')
                vars()[Dynamic_Variable_Name].write(reading)
                vars()[Dynamic_Variable_Name].close()

with open('tempSensors.py', 'w') as f:
    for line in tempSensors:
        f.write(f"{line}\n")


#find the project_id from the GCP credentials
#gcpJson = open('friendly-hangar-300513-ab505264c787.json')
gcpJson = open(GCP_CredentialFile)        
gcpData = json.load(gcpJson)
project_id = gcpData["project_id"]


for sensor in tempSensors:
    localSensor = "DataFiles/sensorOut_" + sensor + ".json"
    sensorURL = "timeseries_AI/" + sensor + ".json"
    with open(f'{localSensor}', 'rb+') as o:
        o.seek(-1, 2)
        o.truncate()
    try:
        
        gcpUpload.delete_blob(GCP_storageProject,sensorURL,GCP_CredentialFile)
    
    except:
        pass

    gcpUpload.upload_blob(GCP_storageProject,localSensor,sensorURL,GCP_CredentialFile)

print("Creating TSI Datasets")
createTSI.upload()
print("Appending Real Time data to Datasets")
appendTSIv6.dataCalls()