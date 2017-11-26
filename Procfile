web: env PYTHONPATH=$PYTHONPATH:caiotweb daphne caiotweb.asgi:channel_layer --port $PORT --bind 0.0.0.0 -v2
worker: python caiotweb/manage.py runworker -v2