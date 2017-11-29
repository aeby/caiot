"""
Parse the body JSON string
"""
from json import loads


def process(msg):
    return loads(msg)
