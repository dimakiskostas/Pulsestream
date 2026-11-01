"""The consumer loop decides *when* a batch is handed over; a recorder stands in for *what*."""

import asyncio
import json
import threading

import pytest

from pulsestream.models import RawEvent
from pulsestream.pipeline import BatchResult, run_consumer, should_flush
from tests.factories import _raw, create_payload


def test_should_flush():
    assert should_flush(5, 2.0, 1.0)
    assert not should_flush(0, 2.0, 1.0)
    assert should_flush(5, 1.0, 1.0)
    assert not should_flush(5, 1.0, 2.0)
    assert not should_flush(0, 2.0, 2.0)
    assert not should_flush(0, 1.0, 2.0)


def make_raws(n: int) -> list[RawEvent]:
    """``n`` valid edits with distinct event ids."""
    return [_raw(create_payload(meta={"id": f"evt-{i:04d}"})) for i in range(n)]


class Recorder:
    """A Processor that records the event ids of every batch it is handed."""

    def __init__(self) -> None:
        self.batches: list[list[str]] = []

    def process(self, raws: list[RawEvent]) -> BatchResult:
        self.batches.append([json.loads(r.raw_payload)["meta"]["id"] for r in raws])
        return BatchResult(raw=len(raws), kept=len(raws))


@pytest.fixture
def recorder() -> Recorder:
    return Recorder()


async def test_consumer_flushes_full_batches_exactly_once(recorder: Recorder) -> None:
    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue()
    for raw in make_raws(12):
        await queue.put(raw)
    await queue.put(None)

    await run_consumer(queue, recorder, batch_size=5, flush_interval=60.0)

    assert [len(b) for b in recorder.batches] == [5, 5, 2]
    flat = [eid for b in recorder.batches for eid in b]
    assert len(flat) == len(set(flat)) == 12, "a flushed batch must never be handed over again"


async def test_consumer_flushes_on_time_when_batch_is_not_full(recorder: Recorder) -> None:
    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue()
    for raw in make_raws(3):
        await queue.put(raw)
    task = asyncio.create_task(run_consumer(queue, recorder, batch_size=100, flush_interval=0.05))

    # Poll rather than sleep a fixed time: a loaded CI box can delay the flush timer.
    for _ in range(500):
        if recorder.batches:
            break
        await asyncio.sleep(0.01)
    assert recorder.batches == [[f"evt-{i:04d}" for i in range(3)]]

    await queue.put(None)
    await asyncio.wait_for(task, timeout=2)
    assert recorder.batches == [[f"evt-{i:04d}" for i in range(3)]], "no empty flush on shutdown"


async def test_consumer_never_hands_over_empty_batches(recorder: Recorder) -> None:
    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue()
    task = asyncio.create_task(run_consumer(queue, recorder, batch_size=10, flush_interval=0.02))
    await asyncio.sleep(0.15)  # several flush intervals pass with nothing to write
    await queue.put(None)
    await asyncio.wait_for(task, timeout=2)
    assert recorder.batches == []


async def test_processing_runs_on_a_worker_thread_not_the_event_loop() -> None:
    class ThreadRecorder(Recorder):
        def __init__(self) -> None:
            super().__init__()
            self.threads: set[int] = set()

        def process(self, raws: list[RawEvent]) -> BatchResult:
            self.threads.add(threading.get_ident())
            return super().process(raws)

    recorder = ThreadRecorder()
    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue()
    for raw in make_raws(4):
        await queue.put(raw)
    await queue.put(None)

    await run_consumer(queue, recorder, batch_size=2, flush_interval=60.0)

    assert [len(b) for b in recorder.batches] == [2, 2]
    assert recorder.threads and threading.get_ident() not in recorder.threads


async def test_consumer_passes_raw_events_through_untouched(recorder: Recorder) -> None:
    """Filtering is the processor's job: the loop hands over every raw event, valid or not."""
    queue: asyncio.Queue[RawEvent | None] = asyncio.Queue()
    await queue.put(_raw(create_payload(meta={"id": "keep-1"})))
    await queue.put(_raw(create_payload(meta={"id": "drop-log"}, type="log")))
    await queue.put(_raw(create_payload(meta={"id": "keep-2"})))
    await queue.put(None)

    await run_consumer(queue, recorder, batch_size=50, flush_interval=60.0)

    assert recorder.batches == [["keep-1", "drop-log", "keep-2"]]
