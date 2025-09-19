import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pipeline import Pipeline
from stage import work_pool, mix_pool
from worker_state import WorkerState
from util import is_ok


# Multiprocessing-compatible handlers must be defined at module scope on macOS.
def _mp_square(x: int, state: WorkerState) -> int:
    return x * x


def _collect_print_values(mock_print: MagicMock) -> list[Any]:
    return [call.args[0] for call in mock_print.call_args_list if call.args]


async def _run_and_collect(pipeline: Pipeline) -> tuple[bool, list[Any]]:
    pipeline.log = True
    with patch("builtins.print") as mock_print:
        result = await pipeline.run()
    return is_ok(result), _collect_print_values(mock_print)


@pytest.mark.asyncio
async def test_pipeline_results_with_buffers_and_chained_stages():
    @work_pool(buffer=2)
    def add_one(x: int, state: WorkerState) -> int:
        return x + 1

    @work_pool()
    def double(x: int, state: WorkerState) -> int:
        return x * 2

    data = [1, 2, 3]
    pipeline = Pipeline(iter(data)) >> add_one >> double

    ok, outputs = await _run_and_collect(pipeline)

    assert ok
    assert outputs == [4, 6, 8]


@pytest.mark.asyncio
async def test_pipeline_results_with_retries_and_multiple_workers():
    @work_pool(num_workers=2, retries=2)
    def flaky_times_ten(x: int, state: WorkerState) -> int:
        attempts = state.get("attempts", {})
        count = attempts.get(x, 0) + 1
        attempts[x] = count
        state.update(attempts=attempts)
        if count == 1:
            raise ValueError("first attempt fails")
        return x * 10

    @work_pool()
    def add_one(x: int, state: WorkerState) -> int:
        return x + 1

    data = [1, 2, 3, 4]
    pipeline = Pipeline(iter(data)) >> flaky_times_ten >> add_one

    ok, outputs = await _run_and_collect(pipeline)

    assert ok
    assert sorted(outputs) == [11, 21, 31, 41]


@pytest.mark.asyncio
async def test_pipeline_results_with_multiprocessing_stage():
    mp_stage = work_pool(buffer=3, num_workers=2, multi_proc=True)(_mp_square)

    @work_pool()
    def subtract_one(x: int, state: WorkerState) -> int:
        return x - 1

    data = [2, 3, 4]
    pipeline = Pipeline(iter(data)) >> mp_stage >> subtract_one

    ok, outputs = await _run_and_collect(pipeline)

    assert ok
    assert sorted(outputs) == [3, 8, 15]


@pytest.mark.asyncio
async def test_pipeline_results_with_mix_pool_and_fork_merge():
    @work_pool()
    def preprocess(x: int, state: WorkerState) -> int:
        return x + 1

    @mix_pool(buffer=2, fork_merge=sum)
    def fan_out():
        return [
            lambda x, state: x * 2,
            lambda x, state: x * 3,
        ]

    @work_pool()
    def finalize(x: int, state: WorkerState) -> int:
        return x - 1

    data = [1, 2]
    pipeline = Pipeline(iter(data)) >> preprocess >> fan_out >> finalize

    ok, outputs = await _run_and_collect(pipeline)

    assert ok
    assert outputs == [9, 14]


@pytest.mark.asyncio
async def test_pipeline_results_with_async_stages():
    @work_pool(buffer=2)
    async def async_increment(x: int, state: WorkerState) -> int:
        await asyncio.sleep(0.001)
        return x + 1

    @work_pool(buffer=2)
    async def async_double(x: int, state: WorkerState) -> int:
        await asyncio.sleep(0.001)
        return x * 2

    data = range(5)
    pipeline = Pipeline(iter(data)) >> async_increment >> async_double

    ok, outputs = await _run_and_collect(pipeline)

    assert ok
    assert outputs == [2, 4, 6, 8, 10]
