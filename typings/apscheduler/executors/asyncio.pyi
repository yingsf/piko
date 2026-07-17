from datetime import datetime

from apscheduler.job import Job

class AsyncIOExecutor:
    def _do_submit_job(self, job: Job, run_times: list[datetime]) -> None: ...
