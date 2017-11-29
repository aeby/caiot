import redis
from django.conf import settings

con = redis.Redis.from_url(settings.REDIS)


def get_reply_channel(device_id):
    return con.get(device_id)


def set_reply_channel(device_id, reply_channel):
    return con.set(device_id, reply_channel)


def delete_reply_channel(device_id):
    return con.delete(device_id)
