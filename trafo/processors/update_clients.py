"""
Parse the body JSON string
"""
import json

from channels import Channel

from caiotweb.rt import get_reply_channel


def process(msg):
    reply_channel = get_reply_channel(msg['deviceId'])
    if reply_channel:
        channel = Channel(reply_channel)
        channel.send({'text': json.dumps(msg)})
    return msg
