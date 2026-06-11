import os
import signal
from multiprocessing import Process
from time import sleep
from unittest.mock import patch

from rq.connections import get_connection_kwargs, parse_connection
from rq.job import JobStatus
from rq.queue import Queue
from rq.serializers import JSONSerializer
from rq.worker import SimpleWorker
from rq.worker_pool import WorkerPool, run_worker
from tests import RQTestCase
from tests.fixtures import CustomJob, CustomQueue, _send_shutdown_command, long_running_job, say_hello


def wait_and_send_shutdown_signal(pid, time_to_wait=0.0):
    sleep(time_to_wait)
    os.kill(pid, signal.SIGTERM)


class TestWorkerPool(RQTestCase):
    def test_queues(self):
        """Test queue parsing"""
        pool = WorkerPool(['default', 'foo'], connection=self.connection)
        self.assertEqual(
            set(pool.queues), {Queue('default', connection=self.connection), Queue('foo', connection=self.connection)}
        )

    # def test_spawn_workers(self):
    #     """Test spawning workers"""
    #     pool = WorkerPool(['default', 'foo'], connection=self.connection, num_workers=2)
    #     pool.start_workers(burst=False)
    #     self.assertEqual(len(pool.worker_dict.keys()), 2)
    #     pool.stop_workers()

    def test_check_workers(self):
        """Test check_workers()"""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=2)
        pool.start_workers(burst=False)

        # There should be two workers
        pool.check_workers()
        self.assertEqual(len(pool.worker_dict.keys()), 2)

        worker_data = list(pool.worker_dict.values())[0]
        sleep(0.5)
        _send_shutdown_command(worker_data.name, get_connection_kwargs(self.connection), delay=0)
        # 1 worker should be dead since we sent a shutdown command
        sleep(0.75)
        pool.check_workers(respawn=False)
        self.assertEqual(len(pool.worker_dict.keys()), 1)

        # If we call `check_workers` with `respawn=True`, the worker should be respawned
        pool.check_workers(respawn=True)
        self.assertEqual(len(pool.worker_dict.keys()), 2)

        pool.stop_workers()

    def test_reap_workers(self):
        """Dead workers are removed from worker_dict"""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=2)
        pool.start_workers(burst=False)

        # There should be two workers
        pool.reap_workers()
        self.assertEqual(len(pool.worker_dict.keys()), 2)

        worker_data = list(pool.worker_dict.values())[0]
        sleep(0.5)
        _send_shutdown_command(worker_data.name, get_connection_kwargs(self.connection), delay=0)
        # 1 worker should be dead since we sent a shutdown command
        sleep(0.75)
        pool.reap_workers()
        self.assertEqual(len(pool.worker_dict.keys()), 1)
        pool.stop_workers()

    def test_start(self):
        """Test start()"""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=2)

        p = Process(target=wait_and_send_shutdown_signal, args=(os.getpid(), 0.5))
        p.start()
        pool.start()
        self.assertEqual(pool.status, pool.Status.STOPPED)
        self.assertTrue(pool.all_workers_have_stopped())
        # We need this line so the test doesn't hang
        pool.stop_workers()

    def test_pool_ignores_consecutive_shutdown_signals(self):
        """If two shutdown signals are sent within one second, only the first one is processed"""
        # Send two shutdown signals within one second while the worker is
        # working on a long running job. The job should still complete (not killed)
        pool = WorkerPool(['foo'], connection=self.connection, num_workers=2)

        process_1 = Process(target=wait_and_send_shutdown_signal, args=(os.getpid(), 0.5))
        process_1.start()
        process_2 = Process(target=wait_and_send_shutdown_signal, args=(os.getpid(), 0.5))
        process_2.start()

        queue = Queue('foo', connection=self.connection)
        job = queue.enqueue(long_running_job, 1)
        pool.start(burst=True)

        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)
        # We need this line so the test doesn't hang
        pool.stop_workers()

    def test_run_worker(self):
        """Ensure run_worker() properly spawns a Worker"""
        queue = Queue('foo', connection=self.connection)
        queue.enqueue(say_hello)

        connection_class, pool_class, pool_kwargs = parse_connection(self.connection)
        run_worker('test-worker', ['foo'], connection_class, pool_class, pool_kwargs)
        # Worker should have processed the job
        self.assertEqual(len(queue), 0)

    def test_worker_pool_arguments(self):
        """Ensure arguments are properly used to create the right workers"""
        queue = Queue('foo', connection=self.connection)
        job = queue.enqueue(say_hello)
        pool = WorkerPool([queue], connection=self.connection, num_workers=2, worker_class=SimpleWorker)
        pool.start(burst=True)
        # Worker should have processed the job
        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)

        queue = Queue('json', connection=self.connection, serializer=JSONSerializer)
        job = queue.enqueue(say_hello, 'Hello')
        pool = WorkerPool(
            [queue], connection=self.connection, num_workers=2, worker_class=SimpleWorker, serializer=JSONSerializer
        )
        pool.start(burst=True)
        # Worker should have processed the job
        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)

        pool = WorkerPool([queue], connection=self.connection, num_workers=2, job_class=CustomJob)
        pool.start(burst=True)
        # Worker should have processed the job
        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)

    def test_worker_pool_custom_queue_class_burst(self):
        """WorkerPool correctly passes custom queue_class to workers in burst mode."""
        queue = CustomQueue('custom_q', connection=self.connection)
        job = queue.enqueue(say_hello)
        pool = WorkerPool(
            [queue],
            connection=self.connection,
            num_workers=1,
            worker_class=SimpleWorker,
            queue_class=CustomQueue,
        )
        pool.start(burst=True)
        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)

    def test_worker_pool_custom_queue_class_non_burst(self):
        """WorkerPool correctly passes custom queue_class to workers in non-burst mode."""
        queue = CustomQueue('custom_q_nonburst', connection=self.connection)
        pool = WorkerPool(
            [queue],
            connection=self.connection,
            num_workers=1,
            worker_class=SimpleWorker,
            queue_class=CustomQueue,
        )
        pool.start_workers(burst=False)
        sleep(0.5)
        # Enqueue after workers are up
        job = queue.enqueue(say_hello)
        # Give worker time to process
        sleep(1.5)
        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)
        pool.stop_workers()

    def test_worker_pool_custom_job_class_non_burst(self):
        """WorkerPool correctly passes custom job_class to workers in non-burst mode."""
        queue = Queue('custom_job_nonburst', connection=self.connection)
        job = queue.enqueue(say_hello)
        pool = WorkerPool(
            [queue],
            connection=self.connection,
            num_workers=1,
            worker_class=SimpleWorker,
            job_class=CustomJob,
        )
        pool.start_workers(burst=False)
        sleep(2)
        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)
        pool.stop_workers()

    def test_worker_pool_custom_serializer_non_burst(self):
        """WorkerPool correctly passes custom serializer to workers in non-burst mode."""
        queue = Queue('custom_ser_nonburst', connection=self.connection, serializer=JSONSerializer)
        job = queue.enqueue(say_hello, 'World')
        pool = WorkerPool(
            [queue],
            connection=self.connection,
            num_workers=1,
            worker_class=SimpleWorker,
            serializer=JSONSerializer,
        )
        pool.start_workers(burst=False)
        sleep(2)
        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)
        pool.stop_workers()

    def test_worker_pool_all_custom_classes_burst(self):
        """WorkerPool correctly passes all custom classes together in burst mode."""
        queue = CustomQueue(
            'all_custom', connection=self.connection, job_class=CustomJob, serializer=JSONSerializer
        )
        job = queue.enqueue(say_hello, 'AllCustom')
        pool = WorkerPool(
            [queue],
            connection=self.connection,
            num_workers=1,
            worker_class=SimpleWorker,
            job_class=CustomJob,
            queue_class=CustomQueue,
            serializer=JSONSerializer,
        )
        pool.start(burst=True)
        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)

    def test_run_worker_forwards_custom_classes(self):
        """run_worker() passes custom queue_class, job_class and serializer to the Worker."""
        queue = Queue('run_worker_cls', connection=self.connection)
        queue.enqueue(say_hello)

        connection_class, pool_class, pool_kwargs = parse_connection(self.connection)

        captured = {}

        original_work = SimpleWorker.work

        def fake_work(self_worker, *args, **kwargs):
            captured['queue_class'] = self_worker.queue_class
            captured['job_class'] = self_worker.job_class
            captured['serializer'] = self_worker.serializer
            return original_work(self_worker, *args, **kwargs)

        with patch.object(SimpleWorker, 'work', fake_work):
            run_worker(
                'test-cls',
                ['run_worker_cls'],
                connection_class,
                pool_class,
                pool_kwargs,
                worker_class=SimpleWorker,
                job_class=CustomJob,
                queue_class=CustomQueue,
                serializer=JSONSerializer,
            )

        self.assertIs(captured['queue_class'], CustomQueue)
        self.assertIs(captured['job_class'], CustomJob)
        self.assertIs(captured['serializer'], JSONSerializer)

    def test_get_worker_process_passes_queue_class(self):
        """WorkerPool.get_worker_process() includes queue_class in the kwargs forwarded to run_worker."""
        pool = WorkerPool(
            ['q1'],
            connection=self.connection,
            num_workers=1,
            worker_class=SimpleWorker,
            job_class=CustomJob,
            queue_class=CustomQueue,
            serializer=JSONSerializer,
        )

        # We don't actually start the process – we just inspect the Process object.
        proc = pool.get_worker_process(name='inspect', burst=True)
        # ForkProcess stores target kwargs in the process object; peek at them.
        kwargs = proc._kwargs  # type: ignore[attr-defined]
        self.assertIs(kwargs['queue_class'], CustomQueue)
        self.assertIs(kwargs['job_class'], CustomJob)
        self.assertIs(kwargs['worker_class'], SimpleWorker)
        self.assertEqual(kwargs['serializer'], JSONSerializer)

