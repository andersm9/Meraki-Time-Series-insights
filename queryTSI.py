import requests

import os
import google.auth
import google.auth.transport.requests
from google.oauth2 import service_account
import requests
from pprint import pprint
from datetime import datetime
import time
from PyInquirer import prompt
import json
from pprint import pprint
import config
import webex



credentials = service_account.Credentials.from_service_account_file(config.GCP_CredentialFile, scopes=['https://www.googleapis.com/auth/cloud-platform'])
auth_req = google.auth.transport.requests.Request()
credentials.refresh(auth_req)
credentials.token

gcpJson = open(config.GCP_CredentialFile)        
gcpData = json.load(gcpJson)
project_id = gcpData["project_id"]


### open the tempSensors file and iterate everything over each dataset
### automatically generate the queryTime (probably based on the forecastHistory / granularity)
### insert the forecastHistory / granularity as variables
#Open the tempSensor file:

"""
with open("tempSensors.py") as t:
    tempSensors = t.read().splitlines()

markdown = "["

for line in tempSensors:
    start = "{'name': '"
    end = "'},\n"
    markdown += start + line + end

markdown += "]"  

markdown = eval(markdown)

###Gather user input

questions = [
    {
        'type': 'checkbox',
        'message': 'Select Sensor',
        'name': 'Sensors',
        'choices': 
            markdown
        ,
        'validate': lambda answer: 'You must choose at least one.' \
            if len(answer) == 0 else True
    },
    {
        'type': 'checkbox',
        'message': 'Select timescale',
        'name': 'timescale',
        'choices': [
            {'name': '120000'},
            {'name': '1200000'},
        ],
        'validate': lambda answer: 'You must choose at least one.' \
            if len(answer) == 0 else True
    }
]
pprint(questions)
answers = prompt(questions)
#print(answers['Sensors'][0])
"""
def ask(Sensor):
  #This sets the duration for which the TSI will use to train the model
  Timescale = 120000

  #set the anomaly detection timespan details
  intForcastHistory = int(Timescale)
  intGranularity = intForcastHistory/1000

  forecastHistory = str(intForcastHistory) + "s"
  granularity = str(intGranularity) + "s"

  url = url = f"https://timeseriesinsights.googleapis.com/v1/projects/{project_id}/datasets/{Sensor}:query"

  #set the time to a recent point
  now=(time.time() - 3600) - intGranularity
  datetime_obj=str(datetime.utcfromtimestamp(now))
  time1 = datetime_obj[:10] + "T" + datetime_obj[11:]
  time2 = time1[:19] + "+00:00"
  queryTime = time2

  payload1 = '''{"detectionTime": "'''

  payload2 = '''",
    "numReturnedSlices": 2,
    "slicingParams": {
      "dimensionNames": ["serial"]
    },
      "timeseriesParams": {
  '''
  payload3 = f'''
      "forecastHistory": "{forecastHistory}",
      "granularity": "{granularity}",
  '''
  payload4 = '''
      "metricAggregationMethod": "AVERAGE",
      "metric": "temperature"
    },
      "forecastParams": {
      "noiseThreshold": 0.0,
      "seasonalityHint": "DAILY"
    },'''
  payload5 = '''
    "returnTimeseries": "true"
  '''
  payload6 = '''
  }

  '''
  # add in payload 5 to view the timeseries
  payload = payload1 + queryTime + payload2 + payload3 + payload4 + payload6

  headers = {
      "Accept": "application/json",
      "Authorization": f"Bearer {credentials.token}"
  }

  response = requests.request('POST', url, headers=headers, data = payload)

  responseJSON = json.loads(response.text)
  readingTaken = True
  try:
    anomalyScore = responseJSON['slices'][0]['anomalyScore']
  except:
    anomalyScore = -1

  try:
    actualTemp =  responseJSON['slices'][0]["detectionPointActual"]
  except:
     actualTemp = -1

  try:
     forecastTemp = responseJSON['slices'][0]["detectionPointForecast"]
  except:
     forecastTemp = -1

  if anomalyScore == -1 or actualTemp == -1 or forecastTemp == -1:
    readingTaken = False

  print(f"anomaly score = {anomalyScore}")
  if readingTaken == True and anomalyScore > float(config.ANOMALYRATING):
    webex.message(f"Temperature Anomaly Detected, \n actual temp: **{round(actualTemp,2)}**, \n forecast temp: **{round(forecastTemp,2)}**,\n Sensor = {Sensor}")
