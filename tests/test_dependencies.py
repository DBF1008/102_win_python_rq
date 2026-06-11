from multiprocessing import Process

from rq import Queue, SimpleWorker, Worker
from rq.connections import get_connection_kwargs
from rq.job import Dependency, Job, JobStatus
from rq.registry import DeferredJobRegistry
from rq.repeat import Repeat
from rq.utils import current_timestamp
from tests import RQTestCase
from tests.fixtures import check_dependencies_are_met, div_by_zero, kill_horse, long_running_job, say_hello


class TestDependencies(RQTestCase):
    def test_allow_failure_is_persisted(self):
        """Ensure that job.allow_dependency_failures is properly set
        when providing Dependency object to depends_on."""
        dep_job = Job.create(func=say_hello, connection=self.connection)

        # default to False, maintaining current behavior
        job = Job.create(func=say_hello, connection=self.connection, depends_on=Dependency([dep_job]))
        job.save()
        Job.fetch(job.id, connection=self.connection)
        self.assertFalse(job.allow_dependency_failures)

        job = Job.create(
            func=say_hello, connection=self.connection, depends_on=Dependency([dep_job], allow_failure=True)
        )
        job.save()
        job = Job.fetch(job.id, connection=self.connection)
        self.assertTrue(job.allow_dependency_failures)

        jobs = Job.fetch_many([job.id], connection=self.connection)
        self.assertTrue(jobs[0].allow_dependency_failures)

    def test_deferred_task_not_enqueued_when_dependencies_are_not_finished(self):
        job_a = Job.create(say_hello, connection=self.connection)
        job_b = Job.create(say_hello, connection=self.connection)
        job_c = Job.create(say_hello, connection=self.connection, depends_on=[job_a, job_b])
        job_a.save()
        job_b.save()
        job_c.save()

        queue = Queue('default', connection=self.connection)
        queue.enqueue_job(job_c)
        self.assertEqual(JobStatus.DEFERRED, job_c.get_status())
        self.assertEqual(0, queue.count)

        queue.enqueue_job(job_a)
        worker = SimpleWorker([queue], connection=queue.connection)
        worker.work(burst=True)
        self.assertEqual(JobStatus.FINISHED, job_a.get_status())

        # Child job is started but should not!!!
        self.assertEqual(JobStatus.DEFERRED, job_c.get_status(refresh=True))

    def test_job_dependency(self):
        """Enqueue dependent jobs only when appropriate"""
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)

        # enqueue dependent job when parent successfully finishes
        parent_job = q.enqueue(say_hello)
        job = q.enqueue_call(say_hello, depends_on=parent_job)
        w.work(burst=True)
        job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        q.empty()

        # don't enqueue dependent job when parent fails
        parent_job = q.enqueue(div_by_zero)
        job = q.enqueue_call(say_hello, depends_on=parent_job)
        w.work(burst=True)
        job = Job.fetch(job.id, connection=self.connection)
        self.assertNotEqual(job.get_status(), JobStatus.FINISHED)
        q.empty()

        # don't enqueue dependent job when Dependency.allow_failure=False (the default)
        parent_job = q.enqueue(div_by_zero)
        dependency = Dependency(jobs=parent_job)
        job = q.enqueue_call(say_hello, depends_on=dependency)
        w.work(burst=True)
        job = Job.fetch(job.id, connection=self.connection)
        self.assertNotEqual(job.get_status(), JobStatus.FINISHED)

        # enqueue dependent job when Dependency.allow_failure=True
        parent_job = q.enqueue(div_by_zero)
        dependency = Dependency(jobs=parent_job, allow_failure=True)
        job = q.enqueue_call(say_hello, depends_on=dependency)

        job = Job.fetch(job.id, connection=self.connection)
        self.assertTrue(job.allow_dependency_failures)

        w.work(burst=True)
        job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

        # When a failing job has multiple dependents, only enqueue those
        # with allow_failure=True
        parent_job = q.enqueue(div_by_zero)
        job_allow_failure = q.enqueue(say_hello, depends_on=Dependency(jobs=parent_job, allow_failure=True))
        job = q.enqueue(say_hello, depends_on=Dependency(jobs=parent_job, allow_failure=False))
        w.work(burst=True, max_jobs=1)
        self.assertEqual(parent_job.get_status(), JobStatus.FAILED)
        self.assertEqual(job_allow_failure.get_status(), JobStatus.QUEUED)
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)
        q.empty()

        # only enqueue dependent job when all dependencies have finished/failed
        first_parent_job = q.enqueue(div_by_zero)
        second_parent_job = q.enqueue(say_hello)
        dependencies = Dependency(jobs=[first_parent_job, second_parent_job], allow_failure=True)
        job = q.enqueue_call(say_hello, depends_on=dependencies)
        w.work(burst=True, max_jobs=1)
        self.assertEqual(first_parent_job.get_status(), JobStatus.FAILED)
        self.assertEqual(second_parent_job.get_status(), JobStatus.QUEUED)
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)

        # When second job finishes, dependent job should be queued
        w.work(burst=True, max_jobs=1)
        self.assertEqual(second_parent_job.get_status(), JobStatus.FINISHED)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        w.work(burst=True)
        job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

        # Test dependant is enqueued at front
        q.empty()
        parent_job = q.enqueue(say_hello)
        q.enqueue(say_hello, job_id='fake_job_id_1', depends_on=Dependency(jobs=[parent_job]))
        q.enqueue(say_hello, job_id='fake_job_id_2', depends_on=Dependency(jobs=[parent_job], enqueue_at_front=True))
        w.work(burst=True, max_jobs=1)

        self.assertEqual(q.job_ids, ['fake_job_id_2', 'fake_job_id_1'])

    def test_multiple_jobs_with_dependencies(self):
        """Enqueue dependent jobs only when appropriate"""
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)

        # Multiple jobs are enqueued with correct status
        parent_job = q.enqueue(say_hello)
        job_no_deps = Queue.prepare_data(say_hello)
        job_with_deps = Queue.prepare_data(say_hello, depends_on=parent_job)
        jobs = q.enqueue_many([job_no_deps, job_with_deps])
        self.assertEqual(jobs[0].get_status(), JobStatus.QUEUED)
        self.assertEqual(jobs[1].get_status(), JobStatus.DEFERRED)
        w.work(burst=True, max_jobs=1)
        self.assertEqual(jobs[1].get_status(), JobStatus.QUEUED)

        job_with_met_deps = Queue.prepare_data(say_hello, depends_on=parent_job)
        jobs = q.enqueue_many([job_with_met_deps])
        self.assertEqual(jobs[0].get_status(), JobStatus.QUEUED)
        q.empty()

    def test_dependency_list_in_depends_on(self):
        """Enqueue with Dependency list in depends_on"""
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)

        # enqueue dependent job when parent successfully finishes
        parent_job1 = q.enqueue(say_hello)
        parent_job2 = q.enqueue(say_hello)
        job = q.enqueue_call(say_hello, depends_on=[Dependency([parent_job1]), Dependency([parent_job2])])
        w.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_enqueue_job_dependency(self):
        """Enqueue via Queue.enqueue_job() with depencency"""
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)

        # enqueue dependent job when parent successfully finishes
        parent_job = Job.create(say_hello, connection=self.connection)
        parent_job.save()
        job = Job.create(say_hello, connection=self.connection, depends_on=parent_job)
        q.enqueue_job(job)
        w.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)
        q.enqueue_job(parent_job)
        w.work(burst=True)
        self.assertEqual(parent_job.get_status(), JobStatus.FINISHED)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_enqueue_job_dependency_score(self):
        """Ensures that deferred jobs are scored by creation time, not TTL."""
        q = Queue(connection=self.connection)
        parent_job = Job.create(say_hello, connection=self.connection)
        parent_job.save()

        timestamp = current_timestamp()
        job = Job.create(say_hello, connection=self.connection, depends_on=parent_job, ttl=5)
        q.enqueue_job(job)
        score = self.connection.zscore(q.deferred_job_registry.key, job.id)
        self.assertGreater(score, timestamp - 2)
        self.assertLess(score, timestamp + 2)

    def test_dependencies_are_met_if_parent_is_canceled(self):
        """When parent job is canceled, it should be treated as failed"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        job.set_status(JobStatus.CANCELED)
        dependent_job = queue.enqueue(say_hello, depends_on=job)
        # dependencies_are_met() should return False, whether or not
        # parent_job is provided
        self.assertFalse(dependent_job.dependencies_are_met(job))
        self.assertFalse(dependent_job.dependencies_are_met())

    def test_can_enqueue_job_if_dependency_is_deleted(self):
        queue = Queue(connection=self.connection)

        dependency_job = queue.enqueue(say_hello, result_ttl=0)

        w = Worker([queue], connection=self.connection)
        w.work(burst=True)

        assert queue.enqueue(say_hello, depends_on=dependency_job)

    def test_dependencies_are_met_if_dependency_is_deleted(self):
        queue = Queue(connection=self.connection)

        dependency_job = queue.enqueue(say_hello, result_ttl=0)
        dependent_job = queue.enqueue(say_hello, depends_on=dependency_job)

        w = Worker([queue], connection=self.connection)
        w.work(burst=True, max_jobs=1)

        assert dependent_job.dependencies_are_met()
        assert dependent_job.get_status() == JobStatus.QUEUED

    def test_dependencies_are_met_at_execution_time(self):
        queue = Queue(connection=self.connection)
        queue.empty()
        queue.enqueue(say_hello, job_id='A')
        queue.enqueue(say_hello, job_id='B')
        job_c = queue.enqueue(check_dependencies_are_met, job_id='C', depends_on=['A', 'B'])

        job_c.dependencies_are_met()
        w = SimpleWorker([queue], connection=self.connection)
        w.work(burst=True)
        assert job_c.return_value(refresh=True)

    def test_allow_failures_when_work_horse_killed(self):
        """Ensure that allow_failure is respected when a worker is killed"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(long_running_job, 10, horse_pid_key='horse_pid_key')
        job2 = queue.enqueue(say_hello, depends_on=Dependency(jobs=job, allow_failure=True))

        # Wait 1 second before killing the horse to simulate horse terminating unexpectedly
        p = Process(target=kill_horse, args=('horse_pid_key', get_connection_kwargs(self.connection), 1))
        p.start()

        worker = Worker([queue], connection=self.connection)
        worker.work(burst=True)

        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertEqual(job2.get_status(), JobStatus.FINISHED)

    def test_dependency_accepts_single_job(self):
        """Test that Dependency constructor accepts a single Job instance"""
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)

        # Test with single Job instance
        parent_job = q.enqueue(say_hello)
        dependency = Dependency(parent_job)  # Single job, not in a list
        job = q.enqueue_call(say_hello, depends_on=dependency)

        w.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        q.empty()

        # Test with single Job instance and allow_failure=True
        parent_job = q.enqueue(div_by_zero)
        dependency = Dependency(parent_job, allow_failure=True)  # Single job with allow_failure
        job = q.enqueue_call(say_hello, depends_on=dependency)

        w.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        q.empty()

        # Test with single job ID string
        parent_job = q.enqueue(say_hello)
        dependency = Dependency(parent_job.id)  # Single job ID string
        job = q.enqueue_call(say_hello, depends_on=dependency)

        w.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_stopped_job_does_not_enqueue_dependents(self):
        """When a job is stopped (STOPPED status), its dependents should NOT be enqueued.

        This tests the fix for the bug where dependencies_are_met() didn't check
        for STOPPED status, causing dependents to be incorrectly enqueued.
        """
        q = Queue(connection=self.connection)

        parent_job = q.enqueue(say_hello)
        dependent_job = q.enqueue(say_hello, depends_on=parent_job)

        self.assertEqual(dependent_job.get_status(), JobStatus.DEFERRED)

        # Simulate parent job being stopped
        parent_job.set_status(JobStatus.STOPPED)

        # dependencies_are_met should return False when parent is STOPPED
        self.assertFalse(dependent_job.dependencies_are_met(parent_job))
        self.assertFalse(dependent_job.dependencies_are_met())

        # Verify enqueue_dependents does not enqueue the dependent
        q.enqueue_dependents(parent_job)
        self.assertEqual(dependent_job.get_status(), JobStatus.DEFERRED)

    # ── Batch enqueue_many enhancement tests ─────────────────────────────

    def test_enqueue_many_deps_satisfied_internal_pipeline(self):
        """enqueue_many: when parent is already FINISHED, dep jobs are
        immediately enqueued via the internal (auto-created) pipeline."""
        q = Queue(connection=self.connection)
        parent = q.enqueue(say_hello)
        parent.set_status(JobStatus.FINISHED)

        dep_data = Queue.prepare_data(say_hello, depends_on=parent, job_id='dep_met')
        jobs = q.enqueue_many([dep_data])

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].get_status(), JobStatus.QUEUED)
        self.assertIn('dep_met', q.job_ids)

    def test_enqueue_many_deps_unsatisfied_internal_pipeline(self):
        """enqueue_many: when parent is still running, dep jobs remain
        DEFERRED and are registered in DeferredJobRegistry."""
        q = Queue(connection=self.connection)
        parent = q.enqueue(say_hello)  # status = QUEUED

        dep_data = Queue.prepare_data(say_hello, depends_on=parent, job_id='dep_unmet')
        jobs = q.enqueue_many([dep_data])

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].get_status(), JobStatus.DEFERRED)
        self.assertNotIn('dep_unmet', q.job_ids)
        self.assertIn('dep_unmet', q.deferred_job_registry.get_job_ids())

    def test_enqueue_many_mixed_internal_pipeline(self):
        """enqueue_many: mixed batch of no-dep, deps-met, deps-unmet,
        at_front, and repeat jobs via the internal pipeline.

        Validates queue ordering: at_front jobs come first, then FIFO.
        """
        q = Queue(connection=self.connection)

        parent_finished = q.enqueue(say_hello, job_id='parent_done')
        parent_finished.set_status(JobStatus.FINISHED)

        parent_pending = q.enqueue(say_hello, job_id='parent_todo')  # QUEUED

        job_datas = [
            Queue.prepare_data(say_hello, job_id='no_dep_1'),
            Queue.prepare_data(say_hello, job_id='no_dep_front', at_front=True),
            Queue.prepare_data(say_hello, job_id='dep_met_1', depends_on=parent_finished),
            Queue.prepare_data(say_hello, job_id='dep_unmet_1', depends_on=parent_pending),
            Queue.prepare_data(say_hello, job_id='repeat_job', repeat=Repeat(times=2, interval=10)),
        ]

        jobs = q.enqueue_many(job_datas)
        self.assertEqual(len(jobs), 5)

        # no-dep and deps-met → QUEUED; deps-unmet → DEFERRED
        self.assertEqual(jobs[0].get_status(), JobStatus.QUEUED)   # no_dep_1
        self.assertEqual(jobs[1].get_status(), JobStatus.QUEUED)   # no_dep_front
        self.assertEqual(jobs[2].get_status(), JobStatus.QUEUED)   # dep_met_1
        self.assertEqual(jobs[3].get_status(), JobStatus.DEFERRED) # dep_unmet_1
        self.assertEqual(jobs[4].get_status(), JobStatus.QUEUED)   # repeat_job

        # Queue order: at_front first, then FIFO insertion order
        self.assertEqual(
            q.job_ids,
            ['no_dep_front', 'no_dep_1', 'dep_met_1', 'repeat_job'],
        )

        # Deferred registry contains only the unmet job
        self.assertIn('dep_unmet_1', q.deferred_job_registry.get_job_ids())

        # Repeat metadata persisted
        jobs[4].refresh()
        self.assertEqual(jobs[4].repeats_left, 2)
        self.assertEqual(jobs[4].repeat_intervals, [10])

    def test_enqueue_many_mixed_external_pipeline(self):
        """enqueue_many: same mixed batch but with an external pipeline.

        Nothing should be visible in Redis until the caller calls execute().
        """
        q = Queue(connection=self.connection)

        parent_finished = q.enqueue(say_hello, job_id='parent_done')
        parent_finished.set_status(JobStatus.FINISHED)

        parent_pending = q.enqueue(say_hello, job_id='parent_todo')

        job_datas = [
            Queue.prepare_data(say_hello, job_id='no_dep_1'),
            Queue.prepare_data(say_hello, job_id='no_dep_front', at_front=True),
            Queue.prepare_data(say_hello, job_id='dep_met_1', depends_on=parent_finished),
            Queue.prepare_data(say_hello, job_id='dep_unmet_1', depends_on=parent_pending),
            Queue.prepare_data(say_hello, job_id='repeat_job', repeat=Repeat(times=3, interval=5)),
        ]

        with self.connection.pipeline() as pipe:
            jobs = q.enqueue_many(job_datas, pipeline=pipe)

            # Before execute: nothing pushed to the queue yet
            self.assertEqual(q.job_ids, [])

            pipe.execute()

        # After execute: correct statuses
        self.assertEqual(jobs[0].get_status(), JobStatus.QUEUED)
        self.assertEqual(jobs[1].get_status(), JobStatus.QUEUED)
        self.assertEqual(jobs[2].get_status(), JobStatus.QUEUED)
        self.assertEqual(jobs[3].get_status(), JobStatus.DEFERRED)
        self.assertEqual(jobs[4].get_status(), JobStatus.QUEUED)

        self.assertEqual(
            q.job_ids,
            ['no_dep_front', 'no_dep_1', 'dep_met_1', 'repeat_job'],
        )
        self.assertIn('dep_unmet_1', q.deferred_job_registry.get_job_ids())

        jobs[4].refresh()
        self.assertEqual(jobs[4].repeats_left, 3)

    def test_enqueue_many_external_pipeline_deps_unsatisfied(self):
        """enqueue_many with external pipeline: deps-unmet stays DEFERRED
        and transitions to QUEUED once the parent finishes."""
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)

        parent = q.enqueue(say_hello, job_id='parent_todo')

        with self.connection.pipeline() as pipe:
            dep_data = Queue.prepare_data(say_hello, job_id='child_1', depends_on=parent)
            jobs = q.enqueue_many([dep_data], pipeline=pipe)
            pipe.execute()

        self.assertEqual(jobs[0].get_status(), JobStatus.DEFERRED)

        # Process parent → child should transition to QUEUED
        w.work(burst=True, max_jobs=1)
        self.assertEqual(parent.get_status(), JobStatus.FINISHED)
        self.assertEqual(jobs[0].get_status(), JobStatus.QUEUED)

    def test_enqueue_many_external_pipeline_cross_queue(self):
        """enqueue_many across multiple queues within a single external
        pipeline should be atomic — nothing visible until execute()."""
        q1 = Queue(name='q1', connection=self.connection)
        q2 = Queue(name='q2', connection=self.connection)

        with self.connection.pipeline() as pipe:
            jobs1 = q1.enqueue_many(
                [Queue.prepare_data(say_hello, job_id='q1_j1')],
                pipeline=pipe,
            )
            jobs2 = q2.enqueue_many(
                [Queue.prepare_data(say_hello, job_id='q2_j1')],
                pipeline=pipe,
            )
            # Before execute: neither queue has jobs
            self.assertEqual(q1.job_ids, [])
            self.assertEqual(q2.job_ids, [])
            pipe.execute()

        self.assertIn('q1_j1', q1.job_ids)
        self.assertIn('q2_j1', q2.job_ids)

    def test_enqueue_many_allow_failure_in_batch(self):
        """enqueue_many respects allow_failure in a mixed batch:
        dep job with allow_failure=True is enqueued even if parent FAILED."""
        q = Queue(connection=self.connection)

        parent_failed = q.enqueue(div_by_zero, job_id='parent_fail')
        parent_failed.set_status(JobStatus.FAILED)

        dep_allow = Queue.prepare_data(
            say_hello,
            job_id='dep_allow',
            depends_on=Dependency(jobs=parent_failed, allow_failure=True),
        )
        dep_deny = Queue.prepare_data(
            say_hello,
            job_id='dep_deny',
            depends_on=Dependency(jobs=parent_failed, allow_failure=False),
        )

        jobs = q.enqueue_many([dep_allow, dep_deny])

        self.assertEqual(jobs[0].get_status(), JobStatus.QUEUED)   # allow_failure
        self.assertEqual(jobs[1].get_status(), JobStatus.DEFERRED) # deny_failure

    def test_enqueue_many_multiple_deps_batch(self):
        """enqueue_many with several dep jobs sharing the same parent:
        batch WATCH should correctly classify all of them."""
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)

        parent_a = q.enqueue(say_hello, job_id='pa')
        parent_b = q.enqueue(say_hello, job_id='pb')
        parent_a.set_status(JobStatus.FINISHED)
        # parent_b stays QUEUED

        job_datas = [
            Queue.prepare_data(say_hello, job_id='c1', depends_on=parent_a),
            Queue.prepare_data(say_hello, job_id='c2', depends_on=parent_a),
            Queue.prepare_data(say_hello, job_id='c3', depends_on=parent_b),
            Queue.prepare_data(say_hello, job_id='c4', depends_on=[parent_a, parent_b]),
        ]

        jobs = q.enqueue_many(job_datas)

        # c1, c2 met (parent_a finished); c3, c4 unmet (parent_b still queued)
        self.assertEqual(jobs[0].get_status(), JobStatus.QUEUED)   # c1
        self.assertEqual(jobs[1].get_status(), JobStatus.QUEUED)   # c2
        self.assertEqual(jobs[2].get_status(), JobStatus.DEFERRED) # c3
        self.assertEqual(jobs[3].get_status(), JobStatus.DEFERRED) # c4

        # Process parent_b → c3 and c4 should become QUEUED
        w.work(burst=True, max_jobs=1)
        self.assertEqual(parent_b.get_status(), JobStatus.FINISHED)
        self.assertEqual(jobs[2].get_status(), JobStatus.QUEUED)
        self.assertEqual(jobs[3].get_status(), JobStatus.QUEUED)

    def test_enqueue_many_empty_batch(self):
        """enqueue_many with an empty list should be a no-op."""
        q = Queue(connection=self.connection)
        jobs = q.enqueue_many([])
        self.assertEqual(jobs, [])
        self.assertEqual(len(q), 0)

    def test_enqueue_many_only_no_dep_jobs(self):
        """enqueue_many with only no-dep jobs uses a single pipeline round-trip."""
        q = Queue(connection=self.connection)
        job_datas = [
            Queue.prepare_data(say_hello, job_id=f'j{i}') for i in range(5)
        ]
        jobs = q.enqueue_many(job_datas)
        self.assertEqual(len(jobs), 5)
        self.assertEqual(len(q), 5)
        for job in jobs:
            self.assertEqual(job.get_status(), JobStatus.QUEUED)
