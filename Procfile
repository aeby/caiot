web: daphne caiotweb.asgi:channel_layer --port $PORT --bind 0.0.0.0 -v2
channels: python manage.py runworker -v2
trafo: python manage.py runtrafo -v2