import time
from datetime import timedelta
from unittest.mock import patch

from rq import Queue, SimpleWorker
from rq.executions import prepare_execution
from rq.job import Job, JobStatus
from rq.registry import FailedJobRegistry, FinishedJobRegistry, StartedJobRegistry
from rq.repeat import Repeat
from rq.results import Result
from rq.timeouts import (
    TimerDeathPenalty,
    UnixSignalDeathPenalty,
    get_default_death_penalty_class,
)
from rq.utils import now
from tests import RQTestCase
from tests.fixtures import say_hello


class TimerBasedWorker(SimpleWorker):
    death_penalty_class = TimerDeathPenalty


def thread_friendly_sleep_func(seconds):
    end_at = time.time() + seconds
    while True:
        if time.time() > end_at:
            break
        time.sleep(0)


class TestTimeouts(RQTestCase):
    def test_timer_death_penalty(self):
        """Ensure TimerDeathPenalty works correctly."""
        q = Queue(connection=self.connection)
        q.empty()
        finished_job_registry = FinishedJobRegistry(connection=self.connection)
        failed_job_registry = FailedJobRegistry(connection=self.connection)

        # make sure death_penalty_class persists
        w = TimerBasedWorker([q], connection=self.connection)
        self.assertIsNotNone(w)
        self.assertEqual(w.death_penalty_class, TimerDeathPenalty)

        # Test short-running job doesn't raise JobTimeoutException
        job = q.enqueue(thread_friendly_sleep_func, args=(1,), job_timeout=3)
        w.work(burst=True)
        job.refresh()
        self.assertIn(job, finished_job_registry)

        # Test long-running job raises JobTimeoutException
        job = q.enqueue(thread_friendly_sleep_func, args=(5,), job_timeout=3)
        w.work(burst=True)
        self.assertIn(job, failed_job_registry)
        job.refresh()
        self.assertIn('rq.timeouts.JobTimeoutException', job.exc_info)

        # Test negative timeout doesn't raise JobTimeoutException,
        # which implies an unintended immediate timeout.
        job = q.enqueue(thread_friendly_sleep_func, args=(1,), job_timeout=-1)
        w.work(burst=True)
        job.refresh()
        self.assertIn(job, finished_job_registry)

    @patch('rq.timeouts.signal')
    def test_get_default_death_penalty_class(self, mock_signal):
        """get_default_death_penalty_class() returns the correct class."""
        # By default, the mock object has a SIGALRM attribute, so
        # get_default_death_penalty_class returns UnixSignalDeathPenalty
        self.assertTrue(hasattr(mock_signal, 'SIGALRM'))
        self.assertEqual(get_default_death_penalty_class(), UnixSignalDeathPenalty)

        # It should return TimerDeathPenalty when SIGALRM is not available
        delattr(mock_signal, 'SIGALRM')
        self.assertFalse(hasattr(mock_signal, 'SIGALRM'))
        self.assertEqual(get_default_death_penalty_class(), TimerDeathPenalty)


class TestSuccessPath(RQTestCase):
    """Tests for the success completion path in handle_job_success."""

    def test_handle_job_success_normal(self):
        """Normal success: job lands in FinishedJobRegistry, Result is recorded,
        execution is cleaned up, and status is FINISHED."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        worker = SimpleWorker([queue], connection=self.connection)
        worker.register_birth()

        registry = StartedJobRegistry(connection=self.connection)
        job.started_at = now()
        job.ended_at = job.started_at + timedelta(seconds=0.5)
        job._result = 'hello'
        job._status = JobStatus.FINISHED

        prepare_execution(worker, job)
        worker.handle_job_success(job, queue, registry)

        # Status persisted as FINISHED
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

        # Job is in FinishedJobRegistry
        finished_registry = FinishedJobRegistry(connection=self.connection)
        self.assertIn(job, finished_registry)

        # Result record exists
        result = Result.fetch_latest(job)
        self.assertIsNotNone(result)
        self.assertEqual(result.type, Result.Type.SUCCESSFUL)

        # Execution cleaned up
        self.assertIsNone(worker.execution)

        # Stats incremented (refresh from Redis since pipeline wrote there)
        worker.refresh()
        self.assertEqual(worker.successful_job_count, 1)
        self.assertGreater(worker.total_working_time, 0)

    def test_handle_job_success_repeat(self):
        """Repeat success: job is re-enqueued, NOT in FinishedJobRegistry,
        Result is recorded, execution is cleaned up, repeats_left decremented."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello, repeat=Repeat(times=2))
        worker = SimpleWorker([queue], connection=self.connection)
        worker.register_birth()

        # Drain the queue so we can verify re-enqueue
        queue.empty()
        self.assertNotIn(job.id, queue.get_job_ids())

        registry = StartedJobRegistry(connection=self.connection)
        job.started_at = now()
        job.ended_at = job.started_at + timedelta(seconds=0.5)
        job._result = 'hello'
        job._status = JobStatus.FINISHED

        prepare_execution(worker, job)
        worker.handle_job_success(job, queue, registry)

        # Job is re-enqueued (not finished)
        self.assertIn(job.id, queue.get_job_ids())

        # Job is NOT in FinishedJobRegistry
        finished_registry = FinishedJobRegistry(connection=self.connection)
        self.assertNotIn(job, finished_registry)

        # repeats_left decremented
        job.refresh()
        self.assertEqual(job.repeats_left, 1)

        # Result record still exists (records this execution)
        result = Result.fetch_latest(job)
        self.assertIsNotNone(result)
        self.assertEqual(result.type, Result.Type.SUCCESSFUL)

        # Execution cleaned up
        self.assertIsNone(worker.execution)

        # Stats incremented (refresh from Redis since pipeline wrote there)
        worker.refresh()
        self.assertEqual(worker.successful_job_count, 1)

    def test_handle_job_success_with_dependents(self):
        """Success with dependents: dependent job is enqueued after parent completes,
        parent is properly finalized."""
        queue = Queue(connection=self.connection)
        parent_job = queue.enqueue(say_hello)
        dependent_job = queue.enqueue(say_hello, depends_on=parent_job)

        # dependent should be deferred, not in queue
        self.assertEqual(dependent_job.get_status(), JobStatus.DEFERRED)

        worker = SimpleWorker([queue], connection=self.connection)

        # Run only the parent job
        worker.work(burst=True, max_jobs=1)

        # Parent completed successfully
        parent_job = Job.fetch(parent_job.id, connection=self.connection)
        self.assertEqual(parent_job.get_status(), JobStatus.FINISHED)

        # Parent is in FinishedJobRegistry
        finished_registry = FinishedJobRegistry(connection=self.connection)
        self.assertIn(parent_job, finished_registry)

        # Dependent was enqueued
        dependent_job = Job.fetch(dependent_job.id, connection=self.connection)
        self.assertEqual(dependent_job.get_status(), JobStatus.QUEUED)
