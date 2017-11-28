import base64


def transform(msg):
    print('--------------\nMsg: ', base64.decodestring(msg['Body']), '\n')
    return msg
