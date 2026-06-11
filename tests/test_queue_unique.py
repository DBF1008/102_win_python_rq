"""Tests for Queue unique job enqueue behavior."""

from datetime import datetime, timedelta, timezone

from rq import Queue
from rq.exceptions import DuplicateJobError
from rq.job import JobStatus
from tests import RQTestCase
from tests.fixtures import say_hello


class TestEnqueueJobUnique(RQTestCase):
    """Tests for Queue unique enqueue integration."""

    def test_unique_with_dependencies_raises_exception(self):
        """unique=True with job dependencies raises ValueError."""
        queue = Queue(connection=self.connection)

        # First create a dependency job
        dependency_job = queue.enqueue(say_hello, job_id='dependency-job')

        # Try to enqueue a unique job with dependencies
        with self.assertRaises(ValueError) as context:
            queue.enqueue(say_hello, job_id='dependent-job', depends_on=dependency_job, unique=True)

        self.assertIn('unique=True is not supported with job dependencies', str(context.exception))

    def test_schedule_job_unique_raises_on_duplicate(self):
        """schedule_job with unique=True raises DuplicateJobError for duplicate job_id."""
        queue = Queue(connection=self.connection)

        # Create and schedule first job
        job1 = queue.create_job(say_hello, job_id='scheduled-unique-job')
        scheduled_time = datetime.now(timezone.utc) + timedelta(hours=1)
        queue.schedule_job(job1, scheduled_time, unique=True)

        # Verify job is scheduled
        self.assertEqual(job1.get_status(), JobStatus.SCHEDULED)

        # Try to schedule second job with same ID
        job2 = queue.create_job(say_hello, job_id='scheduled-unique-job')
        with self.assertRaises(DuplicateJobError) as context:
            queue.schedule_job(job2, scheduled_time, unique=True)

        self.assertIn('scheduled-unique-job', str(context.exception))

    def test_unique_requires_job_id(self):
        """unique=True without an explicit job_id raises ValueError."""
        queue = Queue(connection=self.connection)

        # enqueue
        with self.assertRaises(ValueError):
            queue.enqueue(say_hello, unique=True)

        # enqueue_job
        job = queue.create_job(say_hello)
        with self.assertRaises(ValueError):
            queue.enqueue_job(job, unique=True)

        # schedule_job
        job = queue.create_job(say_hello)
        scheduled_time = datetime.now(timezone.utc) + timedelta(hours=1)
        with self.assertRaises(ValueError):
            queue.schedule_job(job, scheduled_time, unique=True)


class TestEnqueueManyUnique(RQTestCase):
    """Tests for enqueue_many with unique job support."""

    def test_enqueue_many_unique_jobs(self):
        """enqueue_many with unique=True enqueues jobs successfully."""
        queue = Queue(connection=self.connection)

        job_datas = [
            Queue.prepare_data(say_hello, job_id='unique-1', unique=True),
            Queue.prepare_data(say_hello, job_id='unique-2', unique=True),
        ]
        jobs = queue.enqueue_many(job_datas)

        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0].id, 'unique-1')
        self.assertEqual(jobs[1].id, 'unique-2')
        self.assertEqual(jobs[0].get_status(), JobStatus.QUEUED)
        self.assertEqual(jobs[1].get_status(), JobStatus.QUEUED)
        self.assertEqual(queue.count, 2)

    def test_enqueue_many_mixed_unique_and_regular(self):
        """enqueue_many handles mixed unique and non-unique jobs in the same batch."""
        queue = Queue(connection=self.connection)

        job_datas = [
            Queue.prepare_data(say_hello, job_id='regular-1'),
            Queue.prepare_data(say_hello, job_id='unique-1', unique=True),
            Queue.prepare_data(say_hello, job_id='regular-2'),
            Queue.prepare_data(say_hello, job_id='unique-2', unique=True),
        ]
        jobs = queue.enqueue_many(job_datas)

        self.assertEqual(len(jobs), 4)
        self.assertEqual(jobs[0].id, 'regular-1')
        self.assertEqual(jobs[1].id, 'unique-1')
        self.assertEqual(jobs[2].id, 'regular-2')
        self.assertEqual(jobs[3].id, 'unique-2')
        self.assertEqual(queue.count, 4)

    def test_enqueue_many_unique_duplicate_raises_error(self):
        """enqueue_many raises DuplicateJobError when a unique job already exists."""
        queue = Queue(connection=self.connection)

        # First, enqueue a job to make it exist
        queue.enqueue(say_hello, job_id='existing-job')

        # Now try to batch-enqueue with a duplicate unique job
        job_datas = [
            Queue.prepare_data(say_hello, job_id='new-job'),
            Queue.prepare_data(say_hello, job_id='existing-job', unique=True),
        ]
        with self.assertRaises(DuplicateJobError) as context:
            queue.enqueue_many(job_datas)

        self.assertIn('existing-job', str(context.exception))

    def test_enqueue_many_unique_requires_job_id(self):
        """enqueue_many validates that unique=True requires job_id upfront."""
        queue = Queue(connection=self.connection)

        job_datas = [
            Queue.prepare_data(say_hello, job_id='regular-job'),
            Queue.prepare_data(say_hello, unique=True),  # no job_id
        ]
        with self.assertRaises(ValueError) as context:
            queue.enqueue_many(job_datas)

        self.assertIn('unique=True requires an explicit job_id', str(context.exception))
        # Verify no jobs were enqueued (validation happens upfront)
        self.assertEqual(queue.count, 0)

    def test_enqueue_many_unique_with_depends_on_raises(self):
        """enqueue_many rejects unique=True combined with depends_on upfront."""
        queue = Queue(connection=self.connection)
        dep_job = queue.enqueue(say_hello, job_id='dep-job')

        job_datas = [
            Queue.prepare_data(say_hello, job_id='regular-job'),
            Queue.prepare_data(say_hello, job_id='unique-dep', depends_on=dep_job, unique=True),
        ]
        with self.assertRaises(ValueError) as context:
            queue.enqueue_many(job_datas)

        self.assertIn('unique=True is not supported with job dependencies', str(context.exception))
        # Only dep-job from setup should exist, not the regular-job from the batch
        self.assertEqual(queue.count, 1)

    def test_enqueue_many_unique_with_external_pipeline(self):
        """enqueue_many with unique jobs works when an external pipeline is passed."""
        queue = Queue(connection=self.connection)

        job_datas = [
            Queue.prepare_data(say_hello, job_id='ext-unique-1', unique=True),
            Queue.prepare_data(say_hello, job_id='ext-regular-1'),
        ]

        with self.connection.pipeline() as pipe:
            jobs = queue.enqueue_many(job_datas, pipeline=pipe)
            pipe.execute()

        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0].id, 'ext-unique-1')
        self.assertEqual(jobs[1].id, 'ext-regular-1')
        # Unique job is enqueued via Lua script (immediately), regular via pipeline
        self.assertEqual(jobs[0].get_status(), JobStatus.QUEUED)
        self.assertEqual(jobs[1].get_status(), JobStatus.QUEUED)

    def test_enqueue_many_depends_on_without_unique_still_works(self):
        """Non-unique jobs with depends_on in enqueue_many work normally."""
        queue = Queue(connection=self.connection)
        parent_job = queue.enqueue(say_hello, job_id='parent-job')

        job_datas = [
            Queue.prepare_data(say_hello, job_id='independent-job'),
            Queue.prepare_data(say_hello, job_id='child-job', depends_on=parent_job),
        ]
        jobs = queue.enqueue_many(job_datas)

        self.assertEqual(len(jobs), 2)
        independent = jobs[0]
        dependent = jobs[1]
        self.assertEqual(independent.id, 'independent-job')
        self.assertEqual(independent.get_status(), JobStatus.QUEUED)
        self.assertEqual(dependent.id, 'child-job')
        # Parent is not finished yet, so dependent should be deferred
        self.assertEqual(dependent.get_status(), JobStatus.DEFERRED)

    def test_prepare_data_unique_field(self):
        """prepare_data correctly passes the unique field."""
        data_default = Queue.prepare_data(say_hello)
        self.assertFalse(data_default.unique)

        data_unique = Queue.prepare_data(say_hello, job_id='my-job', unique=True)
        self.assertTrue(data_unique.unique)
        self.assertEqual(data_unique.job_id, 'my-job')
