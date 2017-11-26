from django.db import close_old_connections
from django.dispatch import Signal

transform_started = Signal(providing_args=["environ"])
transform_finished = Signal()
worker_ready = Signal()
worker_process_ready = Signal()

# Connect connection closer to consumer finished as well
transform_finished.connect(close_old_connections)
