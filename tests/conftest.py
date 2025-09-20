"""Pytest configuration and fixtures for parllel tests."""

import pytest
import asyncio
from parllel import Result
from typing import AsyncGenerator, Generator, Any


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def async_queue() -> AsyncGenerator[asyncio.Queue, None]:
    """Provide a fresh async queue for tests."""
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=10)
    yield queue
    # Cleanup: drain any remaining items
    while not queue.empty():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            break


@pytest.fixture
def sample_data() -> list[int]:
    """Provide sample data for testing."""
    return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


@pytest.fixture
def sample_dict_data() -> list[dict[str, str|int]]:
    """Provide sample dictionary data for testing."""
    return [
        {"id": 1, "value": 10, "category": "A"},
        {"id": 2, "value": 20, "category": "B"},
        {"id": 3, "value": 15, "category": "A"},
        {"id": 4, "value": 25, "category": "C"},
    ]


# Pytest markers for organizing tests
def pytest_configure(config: pytest.Config) -> None:
    """Configure custom pytest markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
    config.addinivalue_line("markers", "mp: marks tests that use multiprocessing")


# Custom assertion helpers
def assert_pipeline_result_ok(result: Result) -> None:
    """Assert that a pipeline result is OK."""
    from parllel.util import is_ok
    assert is_ok(result), f"Pipeline result should be OK, got: {result}"


def assert_pipeline_result_error(result: Result) -> None:
    """Assert that a pipeline result is an error."""  
    from parllel.util import is_err
    assert is_err(result), f"Pipeline result should be error, got: {result}"
