from __future__ import unicode_literals

import logging
import multiprocessing
import signal
import sys
import threading
import time

import boto3

from .signals import transform_finished, transform_started, worker_ready

logger = logging.getLogger('caiot.trafo')

sqs = boto3.client('sqs')


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
            response = sqs.receive_message(
                QueueUrl=self.sqs_queue_url,
                AttributeNames=[
                    'SentTimestamp'
                ],
                MaxNumberOfMessages=1,
                MessageAttributeNames=[
                    'All'
                ],
                WaitTimeSeconds=self.wait_time
            )
            self.in_job = True

            # If no messages, stall a little to avoid busy-looping then continue
            if response is None and self.wait_time == 0:
                time.sleep(0.01)
                continue

            # Create message wrapper
            logger.debug("Got message on %s", self.sqs_queue_url)

            try:
                transform_started.send(sender=self.__class__, environ={})
                # logger.debug("Dispatching message on %s to %s", channel, name_that_thing(consumer))
            except:
                logger.exception("Error processing message %s:", response)
            finally:
                # Send consumer finished so DB conns close etc.
                transform_finished.send(sender=self.__class__)

            self.in_job = False


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
