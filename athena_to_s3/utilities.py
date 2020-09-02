import json
import sys

def AddOrUpdateJsonWithValue(key, value, payload):
    new_dict = {}
    if payload == "":
        payload = key + ":" + value
        
    payload = payload.replace("{", "")
    payload = payload.replace("}", "")

    lines = payload.split(",")
    print(lines)
    for line in lines:
        x = line.split(':')[0].replace("\"","")
        y = line.split(':')[1].replace("\"","")
        if y != "" and y != None:
            new_dict[x] = y

    if key in new_dict:
        new_dict.pop(key)

    new_dict[key] = value
            
    return json.dumps(new_dict)

# Grab values from event JSON
def getJsonValue(jsonObj, key):
    if not jsonObj:
        return None
    if not key:
        return None

    value = None
    if key in jsonObj:
        value = jsonObj[key]

    return value