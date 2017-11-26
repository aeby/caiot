import logging

from channels.sessions import channel_session

log = logging.getLogger(__name__)


@channel_session
def ws_connect(message):
    print(message.reply_channel)
    message.reply_channel.send({"accept": True})


@channel_session
def ws_receive(message):
    print(message)


@channel_session
def ws_disconnect(message):
    print(message)


def sensor_update(message):
    print('Sensor', message)
