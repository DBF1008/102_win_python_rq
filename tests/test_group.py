from time import sleep

import pytest

from rq import Queue, SimpleWorker
from rq.exceptions import NoSuchGroupError
from rq.group import Group
from rq.job import Job
from rq.utils import as_text
from tests import RQTestCase
from tests.fixtures import say_hello


class TestGroup(RQTestCase):
    job_1_data = Queue.prepare_data(say_hello, job_id='job1')
    job_2_data = Queue.prepare_data(say_hello, job_id='job2')

    def test_create_group(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        assert isinstance(group, Group)
        assert len(group.get_jobs()) == 2
        q.empty()

    def test_group_cleanup_with_no_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        assert len(group.get_jobs()) == 0
        group.cleanup()
        assert len(group.get_jobs()) == 0
        q.empty()

    def test_group_repr(self):
        group = Group.create(name='foo', connection=self.connection)
        assert group.__repr__() == 'Group(id=foo)'

    def test_group_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        self.assertCountEqual(group.get_jobs(), jobs)
        q.empty()

    def test_fetch_group(self):
        q = Queue(connection=self.connection)
        enqueued_group = Group.create(connection=self.connection)
        enqueued_group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        fetched_group = Group.fetch(enqueued_group.name, self.connection)
        self.assertCountEqual(enqueued_group.get_jobs(), fetched_group.get_jobs())
        assert len(fetched_group.get_jobs()) == 2
        q.empty()

    def test_add_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job2 = group.enqueue_many(q, [self.job_1_data, self.job_2_data])[0]
        assert job2 in group.get_jobs()
        self.assertEqual(job2.group_id, group.name)
        q.empty()

    def test_jobs_added_to_group_key(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job_ids = [job.id for job in group.get_jobs()]
        jobs = list({as_text(job) for job in self.connection.smembers(group.key)})
        self.assertCountEqual(jobs, job_ids)
        q.empty()

    def test_group_id_added_to_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data])
        assert jobs[0].group_id == group.name
        fetched_job = Job.fetch(jobs[0].id, connection=self.connection)
        assert fetched_job.group_id == group.name

    def test_deleted_jobs_removed_from_group(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job = group.get_jobs()[0]
        job.delete()
        group.cleanup()
        redis_jobs = list({as_text(job) for job in self.connection.smembers(group.key)})
        assert job.id not in redis_jobs
        assert job not in group.get_jobs()

    def test_group_added_to_registry(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data])
        redis_groups = {as_text(group) for group in self.connection.smembers('rq:groups')}
        assert group.name in redis_groups
        q.empty()

    @pytest.mark.slow
    def test_expired_jobs_removed_from_group(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [short_lived_job, self.job_1_data])
        w.work(burst=True, max_jobs=1)
        sleep(2)
        w.run_maintenance_tasks()
        group.cleanup()
        assert len(group.get_jobs()) == 1
        assert self.job_1_data.job_id in [job.id for job in group.get_jobs()]
        q.empty()

    @pytest.mark.slow
    def test_empty_group_removed_from_group_list(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [short_lived_job])
        w.work(burst=True, max_jobs=1)
        sleep(2)
        w.run_maintenance_tasks()
        redis_groups = {as_text(group) for group in self.connection.smembers('rq:groups')}
        assert group.name not in redis_groups

    @pytest.mark.slow
    def test_fetch_expired_group_raises_error(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [short_lived_job])
        w.work(burst=True, max_jobs=1)
        sleep(2)
        w.run_maintenance_tasks()
        self.assertRaises(NoSuchGroupError, Group.fetch, group.name, group.connection)
        q.empty()

    def test_get_group_key(self):
        group = Group(name='foo', connection=self.connection)
        self.assertEqual(Group.get_key(group.name), 'rq:group:foo')

    def test_all_returns_all_groups(self):
        q = Queue(connection=self.connection)
        group1 = Group.create(name='group1', connection=self.connection)
        Group.create(name='group2', connection=self.connection)
        group1.enqueue_many(q, [self.job_1_data, self.job_2_data])
        all_groups = Group.all(self.connection)
        assert len(all_groups) == 1
        assert 'group1' in [group.name for group in all_groups]
        assert 'group2' not in [group.name for group in all_groups]

    def test_all_deletes_missing_groups(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data])
        jobs[0].delete()
        assert not self.connection.exists(Group.get_key(group.name))
        assert Group.all(connection=self.connection) == []

    # ------------------------------------------------------------------
    # New tests: batch cleanup for deleted / expired jobs and pipeline
    # commit-boundary guarantees.
    # ------------------------------------------------------------------

    def test_cleanup_removes_multiple_deleted_jobs(self):
        """Deleting several jobs and then running cleanup() must remove all
        of them from the group set — covers the batched-EXISTS path."""
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        job_datas = [Queue.prepare_data(say_hello, job_id=f'batch_del_{i}') for i in range(5)]
        jobs = group.enqueue_many(q, job_datas)
        assert len(group.get_jobs()) == 5

        # Delete 3 of the 5 jobs directly (this calls group.delete_job under
        # the hood, but we also want cleanup to handle any stragglers).
        deleted_ids = {jobs[0].id, jobs[2].id, jobs[4].id}
        for job in [jobs[0], jobs[2], jobs[4]]:
            # Delete from Redis *without* going through group.delete_job so
            # that cleanup() is the mechanism that has to find them.
            self.connection.delete(Job.key_for(job.id))

        group.cleanup()
        remaining_ids = {as_text(j) for j in self.connection.smembers(group.key)}
        assert remaining_ids.isdisjoint(deleted_ids)
        assert len(remaining_ids) == 2
        q.empty()

    @pytest.mark.slow
    def test_cleanup_removes_multiple_expired_jobs(self):
        """Jobs whose result_ttl has expired must all be pruned by a single
        cleanup() pass — exercises the batched path with real TTL expiry."""
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        group = Group.create(connection=self.connection)

        short_lived = [Queue.prepare_data(say_hello, result_ttl=1, job_id=f'expire_{i}') for i in range(3)]
        long_lived = Queue.prepare_data(say_hello, job_id='long_lived')
        group.enqueue_many(q, short_lived + [long_lived])

        w.work(burst=True, max_jobs=3)
        sleep(2)

        group.cleanup()
        remaining_ids = {as_text(j) for j in self.connection.smembers(group.key)}
        assert 'long_lived' in remaining_ids
        assert len(remaining_ids) == 1
        q.empty()

    def test_enqueue_many_external_pipeline_not_executed(self):
        """When the caller supplies their own pipeline, Group.enqueue_many
        must NOT call execute() on it — the caller controls the commit
        boundary."""
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)

        external_pipe = self.connection.pipeline()
        group.enqueue_many(q, [self.job_1_data, self.job_2_data], pipeline=external_pipe)

        # Nothing should have been committed yet.
        assert not self.connection.exists(group.key)
        group_names = {as_text(g) for g in self.connection.smembers(Group.REDIS_GROUP_KEY)}
        assert group.name not in group_names

        # Now the caller commits — everything should appear.
        external_pipe.execute()
        assert self.connection.exists(group.key)
        member_ids = {as_text(j) for j in self.connection.smembers(group.key)}
        assert member_ids == {'job1', 'job2'}
        q.empty()

    def test_enqueue_many_external_pipeline_groups_registered_atomically(self):
        """Multiple groups added via the same external pipeline should all
        appear (or not appear) together — no partial commits."""
        q = Queue(connection=self.connection)
        group_a = Group.create(name='atomic_a', connection=self.connection)
        group_b = Group.create(name='atomic_b', connection=self.connection)

        external_pipe = self.connection.pipeline()
        group_a.enqueue_many(q, [Queue.prepare_data(say_hello, job_id='a1')], pipeline=external_pipe)
        group_b.enqueue_many(q, [Queue.prepare_data(say_hello, job_id='b1')], pipeline=external_pipe)

        # Pre-commit: neither group should be visible.
        assert not self.connection.exists(group_a.key)
        assert not self.connection.exists(group_b.key)

        external_pipe.execute()

        # Post-commit: both groups visible.
        assert self.connection.exists(group_a.key)
        assert self.connection.exists(group_b.key)
        q.empty()

    def test_enqueue_many_batch_with_dependencies(self):
        """Batch-enqueueing jobs with depends_on must add all jobs (both
        queued and deferred) to the group and set group_id on each."""
        q = Queue(connection=self.connection)
        parent = q.enqueue(say_hello, job_id='parent')
        child_data = Queue.prepare_data(say_hello, depends_on=parent, job_id='child')
        independent_data = Queue.prepare_data(say_hello, job_id='independent')

        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [independent_data, child_data])

        job_ids = [j.id for j in jobs]
        assert 'independent' in job_ids
        assert 'child' in job_ids
        assert all(j.group_id == group.name for j in jobs)

        member_ids = {as_text(j) for j in self.connection.smembers(group.key)}
        assert 'independent' in member_ids
        assert 'child' in member_ids
        q.empty()
