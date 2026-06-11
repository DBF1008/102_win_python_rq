from __future__ import annotations

from collections.abc import Iterable
from uuid import uuid4

from redis import Redis
from redis.client import Pipeline

from . import Queue
from .exceptions import NoSuchGroupError
from .job import Job
from .queue import EnqueueData
from .utils import as_text

# Default chunk size for batched EXISTS calls in cleanup().
# Keeps individual Redis commands bounded while still drastically reducing
# round-trips compared to one EXISTS per job.
_CLEANUP_BATCH_SIZE = 1000


class Group:
    """A Group is a container for tracking multiple jobs with a single identifier."""

    REDIS_GROUP_NAME_PREFIX = 'rq:group:'
    REDIS_GROUP_KEY = 'rq:groups'

    def __init__(self, connection: Redis, name: str | None = None):
        self.name = name if name else str(uuid4().hex)
        self.connection = connection
        self.key = f'{self.REDIS_GROUP_NAME_PREFIX}{self.name}'

    def __repr__(self):
        return f'Group(id={self.name})'

    def _add_jobs(self, jobs: Iterable[Job], pipeline: Pipeline):
        """Add jobs to the group's Redis set and register the group in the
        global registry.

        The caller is responsible for executing the pipeline so that external
        pipelines (provided by a caller that wants atomic control over the
        commit boundary) are never submitted behind their back.
        """
        pipeline.sadd(self.key, *[job.id for job in jobs])
        pipeline.sadd(self.REDIS_GROUP_KEY, self.name)

    def cleanup(self):
        """Delete jobs from the group's job registry that have been deleted or
        expired from Redis.

        Uses batched ``EXISTS`` checks (chunked into ``_CLEANUP_BATCH_SIZE``
        pieces) so that groups with thousands of members don't issue one Redis
        command per job.  Each chunk is executed as a single pipeline round
        trip, keeping the number of network round trips proportional to
        ``ceil(N / batch_size)`` instead of ``N``.
        """
        job_ids = [as_text(job) for job in self.connection.smembers(self.key)]
        if not job_ids:
            return

        expired_job_ids: list[str] = []
        for start in range(0, len(job_ids), _CLEANUP_BATCH_SIZE):
            chunk = job_ids[start : start + _CLEANUP_BATCH_SIZE]
            with self.connection.pipeline() as pipe:
                for job_id in chunk:
                    pipe.exists(Job.key_for(job_id))
                results = pipe.execute()
            expired_job_ids.extend(
                job_id for job_id, key_exists in zip(chunk, results) if not key_exists
            )

        if expired_job_ids:
            # SREM in chunks as well so a single command never carries an
            # unbounded argument list.
            for start in range(0, len(expired_job_ids), _CLEANUP_BATCH_SIZE):
                chunk = expired_job_ids[start : start + _CLEANUP_BATCH_SIZE]
                with self.connection.pipeline() as pipe:
                    pipe.srem(self.key, *chunk)
                    pipe.execute()

    def enqueue_many(
        self,
        queue: Queue,
        job_datas: Iterable[EnqueueData],
        pipeline: Pipeline | None = None,
    ):
        """Enqueue multiple jobs and add them to this group.

        When *pipeline* is ``None`` (the default) a new pipeline is created
        internally and executed before the method returns.  When an external
        pipeline is supplied the caller is responsible for calling
        ``pipeline.execute()`` — this method will **not** execute it, which
        keeps the caller in full control of the commit boundary and prevents
        partial commits when the group operations are part of a larger
        transaction.
        """
        pipe = pipeline if pipeline else self.connection.pipeline()

        jobs = queue.enqueue_many(job_datas, group_id=self.name, pipeline=pipe)
        self._add_jobs(jobs, pipeline=pipe)

        if pipeline is None:
            pipe.execute()

        return jobs

    def get_jobs(self) -> list:
        """Return the list of live ``Job`` objects that belong to this group.

        Runs ``cleanup()`` first so that deleted / expired entries are pruned
        before the jobs are fetched.
        """
        self.cleanup()
        job_ids = [as_text(job) for job in self.connection.smembers(self.key)]
        return [job for job in Job.fetch_many(job_ids, self.connection) if job is not None]

    def delete_job(self, job_id: str, pipeline: Pipeline | None = None):
        pipe = pipeline if pipeline else self.connection.pipeline()
        pipe.srem(self.key, job_id)
        if pipeline is None:
            pipe.execute()

    @classmethod
    def create(cls, connection: Redis, name: str | None = None):
        return cls(name=name, connection=connection)

    @classmethod
    def fetch(cls, name: str, connection: Redis):
        """Fetch an existing group from Redis"""
        group = cls(name=name, connection=connection)
        if not connection.exists(Group.get_key(group.name)):
            raise NoSuchGroupError
        return group

    @classmethod
    def all(cls, connection: Redis) -> list[Group]:
        "Returns an iterable of all Groups."
        group_keys = [as_text(key) for key in connection.smembers(cls.REDIS_GROUP_KEY)]
        groups = []
        for key in group_keys:
            try:
                groups.append(cls.fetch(key, connection=connection))
            except NoSuchGroupError:
                connection.srem(cls.REDIS_GROUP_KEY, key)
        return groups

    @classmethod
    def get_key(cls, name: str) -> str:
        """Return the Redis key of the set containing a group's jobs"""
        return cls.REDIS_GROUP_NAME_PREFIX + name

    @classmethod
    def clean_registries(cls, connection: Redis):
        """Loop through groups and delete those that have been deleted.
        If group still has jobs in its registry, delete those that have expired"""
        groups = Group.all(connection=connection)
        with connection.pipeline() as p:
            # Remove expired jobs from groups
            for group in groups:
                group.cleanup()
            p.execute()
            # Remove empty groups from group registry
            for group in groups:
                p.exists(group.key)
            results = p.execute()
            expired_group_ids = []
            for i, key_exists in enumerate(results):
                if not key_exists:
                    expired_group_ids.append(groups[i].name)
            if expired_group_ids:
                p.srem(cls.REDIS_GROUP_KEY, *expired_group_ids)
            p.execute()
