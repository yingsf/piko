from collections.abc import Callable, Sequence
from typing import Any

from apscheduler.job import Job

class AsyncIOScheduler:
    running: bool

    def __init__(
        self,
        *,
        jobstores: dict[str, object] | None = None,
        executors: dict[str, object] | None = None,
        job_defaults: dict[str, object] | None = None,
        timezone: object | None = None,
    ) -> None: ...
    def start(self) -> None: ...
    def shutdown(self, wait: bool = True) -> None: ...
    def get_jobs(self) -> list[Job]: ...
    def get_job(self, job_id: str) -> Job | None: ...
    def remove_job(self, job_id: str) -> None: ...
    def reschedule_job(self, job_id: str, *, trigger: object) -> Job: ...
    def modify_job(self, job_id: str, *, name: str | None = None, **kwargs: object) -> Job: ...
    def add_job(
        self,
        func: Callable[..., Any],
        trigger: object,
        *,
        id: str,
        args: Sequence[object] | None = None,
        name: str | None = None,
        **kwargs: object,
    ) -> Job: ...
