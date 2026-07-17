from typing import Any

class Job:
    id: str
    name: str
    kwargs: dict[str, object]
    executor: str
    misfire_grace_time: int | None
    coalesce: bool
    max_instances: int
    trigger: Any
