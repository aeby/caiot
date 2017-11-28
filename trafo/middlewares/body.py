"""
Decode and parse the body JSON string
"""
import base64
import json


def transform(msg):
    #msg.body = json.loads(base64.decodestring(msg.body))
    return msg
