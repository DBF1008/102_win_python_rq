"""Tests for Queue unique job enqueue behavior."""

from datetime import datetime, timedelta, timezone

from rq import Queue
from rq.exceptions import DuplicateJobError
from rq.job import Job, JobStatus
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
    """Tests for Queue.enqueue_many with unique jobs."""

    # ------------------------------------------------------------------
    # Basic unique batch enqueue
    # ------------------------------------------------------------------
    def test_enqueue_many_unique_basic(self):
        """Unique jobs in a batch are enqueued and appear in the queue."""
        q = Queue(connection=self.connection)
        jobs = q.enqueue_many([
            Queue.prepare_data(say_hello, job_id='u1', unique=True),
            Queue.prepare_data(say_hello, job_id='u2', unique=True),
            Queue.prepare_data(say_hello, job_id='u3', unique=True),
        ])
        self.assertEqual(len(jobs), 3)
        for job in jobs:
            self.assertEqual(job.get_status(refresh=False), JobStatus.QUEUED)
        self.assertEqual(len(q), 3)
        self.assertEqual(q.job_ids, ['u1', 'u2', 'u3'])

    # ------------------------------------------------------------------
    # Duplicate detection — across batches
    # ------------------------------------------------------------------
    def test_enqueue_many_unique_duplicate_raises(self):
        """Re-enqueuing the same unique job_id in a later batch raises DuplicateJobError."""
        q = Queue(connection=self.connection)
        q.enqueue_many([Queue.prepare_data(say_hello, job_id='dup-1', unique=True)])

        with self.assertRaises(DuplicateJobError) as ctx:
            q.enqueue_many([Queue.prepare_data(say_hello, job_id='dup-1', unique=True)])
        self.assertIn('dup-1', str(ctx.exception))

    # ------------------------------------------------------------------
    # Duplicate detection — within the same batch
    # ------------------------------------------------------------------
    def test_enqueue_many_unique_duplicate_within_batch(self):
        """Duplicate job_id inside the same batch raises DuplicateJobError."""
        q = Queue(connection=self.connection)
        with self.assertRaises(DuplicateJobError) as ctx:
            q.enqueue_many([
                Queue.prepare_data(say_hello, job_id='same-id', unique=True),
                Queue.prepare_data(say_hello, job_id='same-id', unique=True),
            ])
        self.assertIn('same-id', str(ctx.exception))

    # ------------------------------------------------------------------
    # Mixed unique + non-unique in the same batch
    # ------------------------------------------------------------------
    def test_enqueue_many_mixed_unique_and_normal(self):
        """A batch can mix unique=True and unique=False entries."""
        q = Queue(connection=self.connection)
        jobs = q.enqueue_many([
            Queue.prepare_data(say_hello, job_id='normal-1'),
            Queue.prepare_data(say_hello, job_id='unique-1', unique=True),
            Queue.prepare_data(say_hello, job_id='normal-2'),
            Queue.prepare_data(say_hello, job_id='unique-2', unique=True),
        ])
        self.assertEqual(len(jobs), 4)
        self.assertEqual(
            [j.id for j in jobs],
            ['normal-1', 'unique-1', 'normal-2', 'unique-2'],
        )
        for job in jobs:
            self.assertEqual(job.get_status(refresh=False), JobStatus.QUEUED)

        # Non-unique entries can still be duplicated later
        q.enqueue_many([Queue.prepare_data(say_hello, job_id='normal-1')])
        # But unique entries cannot
        with self.assertRaises(DuplicateJobError):
            q.enqueue_many([Queue.prepare_data(say_hello, job_id='unique-1', unique=True)])

    # ------------------------------------------------------------------
    # External pipeline interaction
    # ------------------------------------------------------------------
    def test_enqueue_many_unique_with_external_pipeline(self):
        """Unique jobs are enqueued immediately even with an external pipeline.
        Non-unique jobs are deferred until pipe.execute()."""
        q = Queue(connection=self.connection)
        with q.connection.pipeline() as pipe:
            jobs = q.enqueue_many([
                Queue.prepare_data(say_hello, job_id='ext-u1', unique=True),
                Queue.prepare_data(say_hello, job_id='ext-n1'),
                Queue.prepare_data(say_hello, job_id='ext-u2', unique=True),
            ], pipeline=pipe)

            # Unique jobs should already be in the queue (Lua script ran immediately)
            self.assertIn('ext-u1', q.job_ids)
            self.assertIn('ext-u2', q.job_ids)

            # Non-unique jobs are NOT in the queue yet (still in pipeline buffer)
            self.assertNotIn('ext-n1', q.job_ids)

            pipe.execute()

        # After execute, non-unique job is also in the queue
        self.assertIn('ext-n1', q.job_ids)
        self.assertEqual(len(jobs), 3)

    # ------------------------------------------------------------------
    # unique=True + depends_on  →  ValueError
    # ------------------------------------------------------------------
    def test_enqueue_many_unique_with_depends_on_raises(self):
        """unique=True combined with depends_on raises ValueError."""
        q = Queue(connection=self.connection)
        parent = q.enqueue(say_hello, job_id='parent-1')

        with self.assertRaises(ValueError) as ctx:
            q.enqueue_many([
                Queue.prepare_data(say_hello, job_id='child-1', unique=True, depends_on=parent),
            ])
        self.assertIn('unique=True is not supported with job dependencies', str(ctx.exception))

    # ------------------------------------------------------------------
    # unique=True without job_id  →  ValueError
    # ------------------------------------------------------------------
    def test_enqueue_many_unique_requires_job_id(self):
        """unique=True without an explicit job_id raises ValueError."""
        q = Queue(connection=self.connection)
        with self.assertRaises(ValueError) as ctx:
            q.enqueue_many([
                Queue.prepare_data(say_hello, unique=True),
            ])
        self.assertIn('unique=True requires an explicit job_id', str(ctx.exception))

    # ------------------------------------------------------------------
    # Re-enqueue after job is deleted
    # ------------------------------------------------------------------
    def test_enqueue_many_unique_allows_requeue_after_delete(self):
        """After a unique job is deleted, the same job_id can be re-enqueued."""
        q = Queue(connection=self.connection)
        jobs = q.enqueue_many([
            Queue.prepare_data(say_hello, job_id='re-u1', unique=True),
        ])
        self.assertEqual(len(jobs), 1)

        # Delete the job from Redis
        jobs[0].delete()

        # Now re-enqueue with the same job_id should succeed
        jobs2 = q.enqueue_many([
            Queue.prepare_data(say_hello, job_id='re-u1', unique=True),
        ])
        self.assertEqual(len(jobs2), 1)
        self.assertEqual(jobs2[0].id, 're-u1')
        self.assertEqual(jobs2[0].get_status(refresh=False), JobStatus.QUEUED)

