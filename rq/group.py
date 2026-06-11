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


class Group:
    """A Group is a container for tracking multiple jobs with a single identifier."""

    REDIS_GROUP_NAME_PREFIX = 'rq:group:'
    REDIS_GROUP_KEY = 'rq:groups'
    CHUNK_SIZE = 1000

    def __init__(self, connection: Redis, name: str | None = None):
        self.name = name if name else str(uuid4().hex)
        self.connection = connection
        self.key = f'{self.REDIS_GROUP_NAME_PREFIX}{self.name}'

    def __repr__(self):
        return f'Group(id={self.name})'

    def _add_jobs(self, jobs: Iterable[Job], pipeline: Pipeline):
        """Add jobs to the group. Does not execute the pipeline."""
        pipeline.sadd(self.key, *[job.id for job in jobs])
        pipeline.sadd(self.REDIS_GROUP_KEY, self.name)

    def cleanup(self):
        """Delete jobs from the group's job registry that have been deleted or expired from Redis."""
        job_ids = [as_text(job) for job in self.connection.smembers(self.key)]
        if not job_ids:
            return

        expired_job_ids = []
        for i in range(0, len(job_ids), self.CHUNK_SIZE):
            chunk = job_ids[i : i + self.CHUNK_SIZE]
            with self.connection.pipeline() as pipe:
                for job_id in chunk:
                    pipe.exists(Job.key_for(job_id))
                results = pipe.execute()
            for j, key_exists in enumerate(results):
                if not key_exists:
                    expired_job_ids.append(chunk[j])

        if expired_job_ids:
            self.connection.srem(self.key, *expired_job_ids)

    def enqueue_many(self, queue: Queue, job_datas: Iterable[EnqueueData], pipeline: Pipeline | None = None):
        pipe = pipeline if pipeline else self.connection.pipeline()

        jobs = queue.enqueue_many(job_datas, group_id=self.name, pipeline=pipe)

        self._add_jobs(jobs, pipeline=pipe)

        if pipeline is None:
            pipe.execute()

        return jobs

    def get_jobs(self) -> list:
        """Retrieve jobs from the group, cleaning up expired entries inline."""
        job_ids = [as_text(job_id) for job_id in self.connection.smembers(self.key)]
        if not job_ids:
            return []

        jobs = Job.fetch_many(job_ids, self.connection)

        expired_job_ids = []
        live_jobs = []
        for i, job in enumerate(jobs):
            if job is None:
                expired_job_ids.append(job_ids[i])
            else:
                live_jobs.append(job)

        if expired_job_ids:
            self.connection.srem(self.key, *expired_job_ids)

        return live_jobs

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
        """Clean up expired jobs from all groups and remove empty groups from the registry."""
        group_names = [as_text(name) for name in connection.smembers(cls.REDIS_GROUP_KEY)]
        if not group_names:
            return

        # Phase 1: batch-check which group keys still exist
        with connection.pipeline() as pipe:
            for name in group_names:
                pipe.exists(cls.REDIS_GROUP_NAME_PREFIX + name)
            results = pipe.execute()

        existing_groups = []
        defunct_names = []
        for i, exists in enumerate(results):
            if exists:
                existing_groups.append(cls(name=group_names[i], connection=connection))
            else:
                defunct_names.append(group_names[i])

        # Phase 2: clean expired jobs from each surviving group
        for group in existing_groups:
            group.cleanup()

        # Phase 3: after cleanup, check which groups are now empty
        if existing_groups:
            with connection.pipeline() as pipe:
                for group in existing_groups:
                    pipe.exists(group.key)
                results = pipe.execute()
            for i, exists in enumerate(results):
                if not exists:
                    defunct_names.append(existing_groups[i].name)

        # Phase 4: remove all defunct groups from the registry
        if defunct_names:
            connection.srem(cls.REDIS_GROUP_KEY, *defunct_names)
