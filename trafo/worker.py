from __future__ import unicode_literals

import logging
import multiprocessing
import signal
import sys
import threading
import time
from importlib import import_module

import boto3
from django.conf import settings
from django.utils import six

from .signals import transform_finished, transform_started, worker_ready

logger = logging.getLogger('caiot.trafo')


class Worker(object):
    """
    A "worker" process that continually looks for available messages on SQS to transform
    and store them. Notifies any connected client via web socket.
    """

    def __init__(
            self,
            sqs_queue_url,
            signal_handlers=True,
            wait_time=20
    ):
        self.sqs_queue_url = sqs_queue_url
        self.signal_handlers = signal_handlers
        self.wait_time = wait_time
        self.termed = False
        self.in_job = False
        self.middlewares = []
        self.__load_middleware()
        self.queue = boto3.resource('sqs').get_queue_by_name(QueueName='caru')

    def __load_middleware(self):
        for middleware_path in settings.TRAFO_MIDDLEWARES:
            module = import_module(middleware_path)
            try:
                self.middlewares.append(getattr(module, 'transform'))
            except AttributeError:
                msg = 'Module "%s" does not define a "transform" attribute' % middleware_path
                six.reraise(ImportError, ImportError(msg), sys.exc_info()[2])

    def install_signal_handler(self):
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)

    def sigterm_handler(self, signo, stack_frame):
        self.termed = True
        if self.in_job:
            logger.info("Shutdown signal received while busy, waiting for loop termination")
        else:
            logger.info("Shutdown signal received while idle, terminating immediately")
            sys.exit(0)

    def ready(self):
        """
        Called once worker setup is complete.
        """
        worker_ready.send(sender=self)

    def run(self):
        """
        Tries to continually transform messages.
        """
        if self.signal_handlers:
            self.install_signal_handler()
        logger.info("Listening on queue %s", self.sqs_queue_url)
        while not self.termed:
            self.in_job = False
            # Long poll for message on provided SQS queue
            messages = self.queue.receive_messages(
                MaxNumberOfMessages=10,
                VisibilityTimeout=30,
                WaitTimeSeconds=20
                # WaitTimeSeconds=self.wait_time
            )
            self.in_job = True
            # If no messages...
            if len(messages) < 1:
                logger.warn("No messages")
                # stall a little to avoid busy-looping if wait time is zero...
                if self.wait_time == 0:
                    time.sleep(0.02)
                # then continue
                continue

            logger.debug("Got %d messages on %s", len(messages), self.sqs_queue_url)

            for message in messages:
                try:

                    transform_started.send(sender=self.__class__, environ={})
                    # logger.debug("Dispatching message on %s to %s", channel, name_that_thing(consumer))
                    for mw in self.middlewares:
                        message = mw(message)
                    print(message.body)
                    message.delete()
                except:
                    logger.exception("Error processing message %s:", message)
                finally:
                    # Send consumer finished so DB conns close etc.
                    transform_finished.send(sender=self.__class__)


class WorkerGroup(Worker):
    """
    Group several workers together in threads. Manages the sub-workers,
    terminating them if a signal is received.
    """

    def __init__(self, *args, **kwargs):
        n_threads = kwargs.pop('n_threads', multiprocessing.cpu_count()) - 1
        super(WorkerGroup, self).__init__(*args, **kwargs)
        kwargs['signal_handlers'] = False
        self.workers = [Worker(*args, **kwargs) for ii in range(n_threads)]

    def sigterm_handler(self, signo, stack_frame):
        logger.info("Shutdown signal received by WorkerGroup, terminating immediately.")
        sys.exit(0)

    def ready(self):
        super(WorkerGroup, self).ready()
        for wkr in self.workers:
            wkr.ready()

    def run(self):
        """
        Launch sub-workers before running.
        """
        self.threads = [threading.Thread(target=self.workers[ii].run)
                        for ii in range(len(self.workers))]
        for t in self.threads:
            t.daemon = True
            t.start()
        super(WorkerGroup, self).run()
