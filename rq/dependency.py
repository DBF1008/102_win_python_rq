from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from redis.client import Pipeline
from redis.exceptions import WatchError

from .job import Job, JobStatus

if TYPE_CHECKING:
    from redis import Redis


class Dependency:
    @classmethod
    def get_jobs_with_met_dependencies(
        cls,
        jobs: Iterable['Job'],
        pipeline: Pipeline,
        is_external_pipeline: bool = False,
        connection: Redis | None = None,
    ):
        """Split *jobs* into two groups based on whether their dependencies are met.

        **Internal pipeline** (``is_external_pipeline=False``, the default):
        All dependency keys are WATCHed in a single call, every job's dependency
        is registered, parent statuses are read through one pipelined HGET batch,
        and a single ``EXEC`` commits the transaction.  This replaces the old
        per-job ``WATCH / MULTI / EXEC`` loop and reduces Redis round-trips from
        *O(N)* to *O(1)*.

        **External pipeline** (``is_external_pipeline=True``):
        The caller is responsible for the transaction boundary so we skip WATCH
        entirely.  Registrations are flushed to Redis immediately (so that a
        parent's ``enqueue_dependents`` can discover the reverse mapping),
        parent statuses are read in one batch via *connection*, and the enqueue
        commands for met-dependency jobs are queued in the caller's pipeline
        without calling ``execute()``.

        Args:
            jobs: Jobs whose dependencies should be checked.
            pipeline: The Redis pipeline to use.
            is_external_pipeline: When *True*, skip WATCH and do not call
                ``pipeline.execute()``; the caller manages the transaction.
            connection: A live ``Redis`` client used for synchronous reads in
                the external-pipeline path.  Required when
                *is_external_pipeline* is ``True``.

        Returns:
            A tuple ``(jobs_with_met_dependencies, jobs_with_unmet_dependencies)``.
        """
        jobs = list(jobs)
        if not jobs:
            return [], []

        jobs_with_met_dependencies: list[Job] = []
        jobs_with_unmet_dependencies: list[Job] = []

        # Collect the unique set of parent IDs across all jobs.
        all_parent_ids: list[str] = []
        seen: set[str] = set()
        for job in jobs:
            for did in job._dependency_ids:
                if did not in seen:
                    seen.add(did)
                    all_parent_ids.append(did)

        if not is_external_pipeline:
            # ── Internal pipeline: single WATCH / MULTI / EXEC ──────────
            all_dep_keys: list[str] = []
            for job in jobs:
                all_dep_keys.extend(Job.key_for(did) for did in job._dependency_ids)

            while True:
                try:
                    if all_dep_keys:
                        pipeline.watch(*all_dep_keys)

                    # Register reverse-mappings (SADDs execute immediately
                    # under WATCH, before MULTI).
                    for job in jobs:
                        job.register_dependency(pipeline=pipeline)

                    # Batch-read every parent's status in one inner pipeline.
                    pipeline.multi()
                    for pid in all_parent_ids:
                        pipeline.hget(Job.key_for(pid), 'status')
                    statuses = pipeline.execute()

                    parent_status_map: dict[str, str | None] = {}
                    for pid, status in zip(all_parent_ids, statuses):
                        parent_status_map[pid] = status.decode() if status else None

                    # Classify each job.
                    for job in jobs:
                        allowed = [JobStatus.FINISHED]
                        if job.allow_dependency_failures:
                            allowed.append(JobStatus.FAILED)
                        if all(parent_status_map.get(did) in allowed for did in job._dependency_ids):
                            jobs_with_met_dependencies.append(job)
                        else:
                            jobs_with_unmet_dependencies.append(job)

                except WatchError:
                    jobs_with_met_dependencies.clear()
                    jobs_with_unmet_dependencies.clear()
                    continue
                break
        else:
            # ── External pipeline: no WATCH, caller controls execute() ──
            if connection is None:
                raise ValueError(
                    'connection is required when is_external_pipeline=True'
                )

            # Flush registrations immediately so that a parent's
            # enqueue_dependents (running concurrently) can discover the
            # reverse-mapping before the caller's pipeline executes.
            for job in jobs:
                job.register_dependency(pipeline=pipeline)
            pipeline.execute()

            # Batch-read parent statuses synchronously via the live client.
            parent_status_map = {}
            if all_parent_ids:
                with connection.pipeline() as read_pipe:
                    for pid in all_parent_ids:
                        read_pipe.hget(Job.key_for(pid), 'status')
                    for pid, status in zip(all_parent_ids, read_pipe.execute()):
                        parent_status_map[pid] = status.decode() if status else None

            # Classify each job.
            for job in jobs:
                allowed = [JobStatus.FINISHED]
                if job.allow_dependency_failures:
                    allowed.append(JobStatus.FAILED)
                if all(parent_status_map.get(did) in allowed for did in job._dependency_ids):
                    jobs_with_met_dependencies.append(job)
                else:
                    jobs_with_unmet_dependencies.append(job)

        return jobs_with_met_dependencies, jobs_with_unmet_dependencies
