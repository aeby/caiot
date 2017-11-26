from __future__ import unicode_literals

from django.conf import settings
from django.core.management import BaseCommand

from trafo.log import setup_logger
from trafo.signals import worker_process_ready
from trafo.worker import Worker, WorkerGroup


class Command(BaseCommand):
    leave_locale_alone = True

    def add_arguments(self, parser):
        super(Command, self).add_arguments(parser)
        parser.add_argument(
            '--threads', action='store', dest='threads',
            default=1, type=int,
            help='Number of threads to execute.'
        )

    def handle(self, *args, **options):
        # Get the backend to use
        self.verbosity = options.get("verbosity", 1)
        self.logger = setup_logger('caiot.trafo', self.verbosity)
        self.n_threads = options.get('threads', 1)
        self.sqs_queue_url = settings.TRAFO_SQS_QUEUE_URL
        # Choose an appropriate worker.
        worker_kwargs = {}
        if self.n_threads == 1:
            self.logger.info("Using single-threaded worker.")
            worker_cls = Worker
        else:
            self.logger.info("Using multi-threaded worker, {} thread(s).".format(self.n_threads))
            worker_cls = WorkerGroup
            worker_kwargs['n_threads'] = self.n_threads
        # Run the worker
        self.logger.info("Running worker against sqs queue %s", self.sqs_queue_url)
        try:
            worker = worker_cls(
                sqs_queue_url=self.sqs_queue_url,
                **worker_kwargs
            )
            worker_process_ready.send(sender=worker)
            worker.ready()
            worker.run()
        except KeyboardInterrupt:
            pass
