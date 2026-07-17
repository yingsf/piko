from pathlib import Path

import pytest
from pydantic import BaseModel

from piko import PikoApp
from piko.config import settings
from piko.infra.db import utcnow
from piko.persistence.intent import WriteIntent
from piko.persistence.sink_base import ResultSink, TypedSink, on
from piko.persistence.writer import PersistenceDeferredError


class MockSink(ResultSink):
    """记录写入意图的测试 Sink"""

    def __init__(self) -> None:
        super().__init__("mock")
        self.written_items: list[WriteIntent] = []

    async def write_batch(self, batch: list[WriteIntent]) -> None:
        """保存收到的批次"""
        self.written_items.extend(batch)


class FailingSink(ResultSink):
    """始终失败的测试 Sink"""

    def __init__(self) -> None:
        super().__init__("failing")

    async def write_batch(self, batch: list[WriteIntent]) -> None:
        """模拟下游存储不可用"""
        raise RuntimeError("sink unavailable")


class UserPayload(BaseModel):
    """测试 TypedSink 恢复使用的模型"""

    name: str


class UserSink(TypedSink):
    """接收 UserPayload 的测试 Sink"""

    def __init__(self) -> None:
        super().__init__("users")
        self.received: list[UserPayload] = []

    @on(UserPayload)
    async def write_users(self, batch: list[UserPayload]) -> None:
        """记录已还原的模型实例"""
        self.received.extend(batch)


@pytest.mark.asyncio
async def test_writer_flow() -> None:
    """验证应用实例持有的 Writer 完成批量写入"""
    app = PikoApp(name="persistence-test")
    sink = MockSink()
    app.writer.register_sink(sink)
    await app.writer.start()

    intent = WriteIntent(
        sink="mock",
        key="test",
        payload="hello",
        idempotency_key="123",
        job_id="test_job",
        scheduled_time=utcnow(),
        run_id=1,
    )

    try:
        await app.writer.enqueue(intent)
        await app.writer.flush()
        assert len(sink.written_items) == 1
        assert sink.written_items[0].payload == "hello"
    finally:
        await app.writer.stop()


@pytest.mark.asyncio
async def test_sink_failure_is_reported_and_spooled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 Sink 失败会落盘并让 flush 报告延迟持久化"""
    fallback_path = tmp_path / "fallback"
    monkeypatch.setattr(settings, "persist_disk_fallback_path", str(fallback_path), raising=False)

    app = PikoApp(name="persistence-failure-test")
    app.writer.register_sink(FailingSink())
    await app.writer.start()
    intent = WriteIntent(
        sink="failing",
        key="failed",
        payload="payload",
        job_id="test_job",
        scheduled_time=utcnow(),
        run_id=1,
    )

    try:
        await app.writer.enqueue(intent)
        with pytest.raises(PersistenceDeferredError):
            await app.writer.flush()
        pending_files = list(tmp_path.glob("fallback*.pending"))
        assert len(pending_files) == 1
        assert WriteIntent.model_validate_json(pending_files[0].read_text().strip()).key == "failed"
    finally:
        await app.writer.stop()


@pytest.mark.asyncio
async def test_recovery_retains_invalid_lines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证恢复坏行时有效数据入队且原始坏行保留"""
    fallback_path = tmp_path / "fallback"
    monkeypatch.setattr(settings, "persist_disk_fallback_path", str(fallback_path), raising=False)
    intent = WriteIntent(
        sink="mock",
        key="recoverable",
        payload="payload",
        job_id="test_job",
        scheduled_time=utcnow(),
        run_id=2,
    )
    pending_path = Path(f"{fallback_path}.manual.pending")
    pending_path.write_text(f"{intent.model_dump_json()}\nnot-json\n", encoding="utf-8")

    app = PikoApp(name="persistence-recovery-test")
    sink = MockSink()
    app.writer.register_sink(sink)
    await app.writer.start()
    try:
        await app.writer.flush()
        assert len(sink.written_items) == 1
        assert not pending_path.exists()
        assert Path(f"{pending_path}.recovered").exists()
        failed_path = Path(f"{pending_path}.failed")
        assert failed_path.read_text(encoding="utf-8").strip() == "not-json"
    finally:
        await app.writer.stop()


@pytest.mark.asyncio
async def test_typed_sink_rehydrates_registered_model() -> None:
    """验证 TypedSink 从 model_ref 还原注册过的模型"""
    sink = UserSink()
    intent = WriteIntent(
        sink="users",
        key="user-1",
        payload={"name": "Ada"},
        model_ref=f"{UserPayload.__module__}:{UserPayload.__qualname__}",
        job_id="test_job",
        scheduled_time=utcnow(),
        run_id=3,
    )

    await sink.write_batch([intent])

    assert sink.received == [UserPayload(name="Ada")]


@pytest.mark.asyncio
async def test_typed_sink_rejects_unknown_model_ref() -> None:
    """验证 TypedSink 不会静默跳过未知模型"""
    sink = UserSink()
    intent = WriteIntent(
        sink="users",
        key="unknown",
        payload={"name": "Ada"},
        model_ref="unknown.module:UnknownModel",
        job_id="test_job",
        scheduled_time=utcnow(),
        run_id=4,
    )

    with pytest.raises(ValueError, match="no registered model"):
        await sink.write_batch([intent])
