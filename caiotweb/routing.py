from . import consumers

channel_routing = {
    # Wire up websocket channels to our consumers:
    'websocket.connect': consumers.ws_connect,
    'websocket.receive': consumers.ws_receive,
    'websocket.disconnect': consumers.ws_disconnect,
}
