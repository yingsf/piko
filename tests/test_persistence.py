import pytest
import asyncio
from piko.persistence.intent import WriteIntent
from piko.persistence.sink_base import ResultSink
from piko.persistence.writer import persistence_writer
from piko.infra.db import utcnow


# 1. Mock Sink
class MockSink(ResultSink):
    def __init__(self):
        self.written_items = []

    @property
    def name(self):
        return "mock"

    async def write_batch(self, intents):
        self.written_items.extend(intents)


@pytest.mark.asyncio
async def test_writer_flow():
    # Setup
    sink = MockSink()
    persistence_writer.register_sink(sink)
    await persistence_writer.start()

    # 1. Enqueue Data
    intent = WriteIntent(
        sink="mock",
        mode="append",
        key="test",
        payload="hello",
        idempotency_key="123",
        job_id="test_job",
        scheduled_time=utcnow(),
        run_id=1
    )

    await persistence_writer.enqueue(intent)

    # 2. Wait for batch flush (timeout 0.5s)
    await asyncio.sleep(0.6)

    # 3. Assert
    assert len(sink.written_items) == 1
    assert sink.written_items[0].payload == "hello"

    # Teardown
    await persistence_writer.stop()
