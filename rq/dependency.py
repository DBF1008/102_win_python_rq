from collections.abc import Iterable

from redis import Redis

from .job import Job, JobStatus


class Dependency:
    @classmethod
    def get_jobs_with_met_dependencies(cls, jobs: Iterable['Job'], connection: Redis):
        jobs_with_met_dependencies = []
        jobs_with_unmet_dependencies = []

        jobs = list(jobs)
        if not jobs:
            return jobs_with_met_dependencies, jobs_with_unmet_dependencies

        # Step 1: Batch register all dependencies in a single pipeline round-trip
        with connection.pipeline() as pipe:
            for job in jobs:
                job.register_dependency(pipeline=pipe)
            pipe.execute()

        # Step 2: Collect all unique dependency IDs and batch-fetch their statuses
        all_dependency_ids = set()
        for job in jobs:
            all_dependency_ids.update(job._dependency_ids)

        dep_status_map = {}
        if all_dependency_ids:
            dep_id_list = list(all_dependency_ids)
            with connection.pipeline() as pipe:
                for dep_id in dep_id_list:
                    pipe.hget(Job.key_for(dep_id), 'status')
                statuses = pipe.execute()
            for dep_id, status in zip(dep_id_list, statuses):
                dep_status_map[dep_id] = status.decode() if status else None

        # Step 3: Classify jobs based on dependency statuses
        # Matches Job.dependencies_are_met semantics: missing deps (None) are treated as met
        for job in jobs:
            allowed_statuses = {JobStatus.FINISHED}
            if job.allow_dependency_failures:
                allowed_statuses.add(JobStatus.FAILED)

            deps_met = all(
                dep_status_map.get(dep_id) in allowed_statuses
                for dep_id in job._dependency_ids
                if dep_status_map.get(dep_id) is not None
            )
            if deps_met:
                jobs_with_met_dependencies.append(job)
            else:
                jobs_with_unmet_dependencies.append(job)

        return jobs_with_met_dependencies, jobs_with_unmet_dependencies
