import requests
import copy
import urllib
import multiprocessing.dummy
import ast
import pandas

# Defining constants and templates

ENV_STG = 1
ENV_PRD = 2

CURR_ENV = 2

url_prd = "https://quijote.apps.prd.us-east-1.bss.olx.org/rest/event"
auth_token_prd = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJxdWlqb3RlIiwiYWN0aW9uIjoic2VuZF9ldmVudCIsInBsYXRmb3JtIjoibW9uZXRpc2F0aW9uX2NsbUBpbmRpYSJ9.39e1yPZe6NNVlE9sNA2Qc_eOOBrTSwr5BXxBtnhFd1w"


url_stg = "https://quijote.apps.stg.us-east-1.bss.olx.org/rest/event"
auth_token_stg = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJxdWlqb3RlIiwiYWN0aW9uIjoic2VuZF9ldmVudCIsInBsYXRmb3JtIjoibW9uZXRpc2F0aW9uX2NsbUBpbmRpYSJ9.f3BpxsyZpICEpzlKd3psZcbGIQqk3I_FR1NoZfZqBq8"

THREAD_COUNT = 20

base_json = {
    "countryCode": "IN",
    "languageCode": "EN",
    "brand": "olx",
    "userId": "",
}

channel_json = {
    'pwa': {
        "type": "push",
        "platform": "pwa",
        "recipientType": "targetId",
        "recipientValue": "",
        "p256dh": "",
        "auth": ""
    },
    'ios': {
        "type": "push",
        "platform": "ios",
        "recipientType": "uaId",
        "recipientValue": ""
    },
    'android': {
        "type": "push",
        "platform": "android",
        "recipientType": "uaId",
        "recipientValue": ""
    },
    'email': {
        "type": "email",
        "address": ""
    },
    'sms': {
        "type": "sms",
        "phone": ""
    }
}

headers_prd = {
    'Authorization': 'Bearer ' + auth_token_prd
}

headers_stg = {
    'Authorization': 'Bearer ' + auth_token_stg
}

def getJson(channel, user, target, params, action):
    json = copy.deepcopy(base_json)
    json["actionKey"] = action
    channel_list = copy.deepcopy(channel_json.get(channel))
    json['userId'] = user
    json['parameters'] = params
    if channel == 'sms':
        channel_list['phone'] = target
    elif channel == 'email':
        channel_list['email'] = target
    elif channel == 'pwa':
        ## Insert PWA Code Here
        channel_list['phone'] = target
    elif channel == 'ios':
        channel_list['recipientValue'] = target
    elif channel == 'android':
        channel_list['recipientValue'] = target
    json['channels'] = [channel_list]
    json['parameters'] = params
    return json

def sendRequest(channel, user, target, params, action):

    if CURR_ENV == ENV_PRD:
        url = url_prd
        headers = headers_prd
    else:
        url = url_stg
        headers = headers_stg

    response = requests.post(url, json=getJson(channel = channel, user = user, target = target, params = params, action = action), headers=headers)
    output = response.json()
    output['user_id'] = user
    output['target'] = target
    return output

def processRequest(row):
    return sendRequest(row[0], row[1], row[2], ast.literal_eval(row[3]), row[4])

def sendBulkRequest(dataFrame):
    tuples = [tuple(x) for x in dataFrame.values]
    p = multiprocessing.dummy.Pool(THREAD_COUNT)
    status = p.map(processRequest, tuples)
    return status




