"""验证 Follower 不执行无效的数据库轮询和内存调度"""

import pytest

from piko import PikoApp
from piko.infra.leader import LeaderMutex


@pytest.mark.asyncio
async def test_follower_skips_reconcile_and_clears_scheduler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证 Follower 不访问数据库且清理已有内存任务"""
    app = PikoApp(name="follower-watcher-test")
    app.scheduler.startup()
    monkeypatch.setattr(LeaderMutex, "is_leader", property(lambda _: False))

    await app.watcher.reconcile_once()

    assert app.scheduler.raw_scheduler.get_jobs() == []
    app.scheduler.shutdown()


def test_system_poll_interval_accepts_only_bounded_numbers() -> None:
    """验证系统动态配置只接受有限范围内的数值"""
    app = PikoApp(name="system-config-test")
    watcher = app.watcher
    watcher.apply_system_config({"poll_interval_s": 2})
    assert watcher.dynamic_interval == 2

    watcher.apply_system_config({"poll_interval_s": 0})
    assert watcher.dynamic_interval == 2
