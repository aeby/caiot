import logging

from channels.sessions import channel_session

from caiotweb.rt import set_reply_channel, delete_reply_channel

log = logging.getLogger(__name__)


class DummyDevice():
    def __init__(self):
        self.device_id = 'SBS01'


dummy = DummyDevice()


# @allowed_hosts_only
# @channel_session_user_from_http
def ws_connect(message):
    # lookup users device
    set_reply_channel(dummy.device_id, message.reply_channel.name)
    message.reply_channel.send({"accept": True})


@channel_session
def ws_receive(message):
    message.reply_channel.send({
        "text": message.content['text'],
    })


@channel_session
def ws_disconnect(message):
    print('Disconnect: %s ' % message.reply_channel.name)
    delete_reply_channel(dummy.device_id)
