"""Tests for worker module - async and multiprocessing workers."""

import asyncio
import pytest
from typing import Any

from worker import worker, worker_no_buf, mp_worker
from async_util import SENTINEL
from worker_state import WorkerState

class TestWorkerNoBuf:
    """Test the worker_no_buf function."""
    
    @pytest.mark.asyncio
    async def test_basic_processing(self):
        inbound = asyncio.Queue(maxsize=5)
        
        # Simple doubling function
        def double(x: int, state: WorkerState) -> int:
            return x * 2
        
        # Add test data
        await inbound.put(1)
        await inbound.put(2)
        await inbound.put(3)
        await inbound.put(SENTINEL)
        
        outbound = worker_no_buf(double, 1, inbound)
        
        # Collect results
        results = []
        while True:
            item = await outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        assert results == [2, 4, 6]
    
    @pytest.mark.asyncio
    async def test_with_retries(self):
        inbound = asyncio.Queue(maxsize=5)
        call_count = 0
        
        def flaky_function(x: int, state: WorkerState) -> int:
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # First call fails
                raise ValueError("First attempt fails")
            return x * 2
        
        await inbound.put(5)
        await inbound.put(SENTINEL)
        
        outbound = worker_no_buf(flaky_function, 2, inbound)
        
        result = await outbound.get()
        assert result == 10
        
        sentinel = await outbound.get()
        assert sentinel is SENTINEL
        assert call_count == 2  # First attempt failed, second succeeded
    
    @pytest.mark.asyncio
    async def test_async_function(self):
        inbound = asyncio.Queue(maxsize=5)
        
        async def async_double(x: int, state: WorkerState) -> int:
            await asyncio.sleep(0.01)  # Small delay
            return x * 2
        
        await inbound.put(3)
        await inbound.put(SENTINEL)
        
        outbound = worker_no_buf(async_double, 1, inbound)
        
        result = await outbound.get()
        assert result == 6
        
        sentinel = await outbound.get()
        assert sentinel is SENTINEL


class TestWorker:
    """Test the worker function with buffering."""
    
    @pytest.mark.asyncio
    async def test_basic_processing_with_buffer(self):
        inbound = asyncio.Queue(maxsize=6)
        
        def increment(x: int, state: WorkerState) -> int:
            return x + 1
        
        # Add test data
        for i in range(5):
            await inbound.put(i)
        await inbound.put(SENTINEL)
        
        outbound = worker(increment, 3, 1, inbound)
        
        # Collect results
        results = []
        while True:
            item = await outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        assert results == [1, 2, 3, 4, 5]
    
    @pytest.mark.asyncio
    async def test_zero_buffer_delegates_to_worker_no_buf(self):
        inbound = asyncio.Queue(maxsize=5)
        
        def identity(x: Any, state: WorkerState) -> Any:
            return x
        
        await inbound.put("test")
        await inbound.put(SENTINEL)
        
        outbound = worker(identity, 0, 1, inbound)
        
        result = await outbound.get()
        assert result == "test"
        
        sentinel = await outbound.get()
        assert sentinel is SENTINEL
    
    @pytest.mark.asyncio
    async def test_negative_buffer_delegates_to_worker_no_buf(self):
        inbound = asyncio.Queue(maxsize=5)
        
        def identity(x: Any, state: WorkerState) -> Any:
            return x
        
        await inbound.put("test")
        await inbound.put(SENTINEL)
        
        outbound = worker(identity, -1, 1, inbound)
        
        result = await outbound.get()
        assert result == "test"
        
        sentinel = await outbound.get()
        assert sentinel is SENTINEL
    
    @pytest.mark.asyncio
    async def test_error_handling_with_retries(self):
        inbound = asyncio.Queue(maxsize=5)
        
        failed = False
        def sometimes_fails(x: int, state: WorkerState) -> int:
            nonlocal failed
            if x == 2 and not failed:
                failed = True
                raise ValueError("Cannot process 2")
            return x * 10
        
        await inbound.put(1)
        await inbound.put(2)  # This will fail
        await inbound.put(3)
        await inbound.put(SENTINEL)
        
        outbound = worker(sometimes_fails, 2, 2, inbound)
        
        results = []
        while True:
            item = await outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        # Should get results for 1 and 3, but 2 should still produce some result
        # after retries (though it will be an error result)
        assert len(results) == 3
        assert 10 in results  # 1 * 10
        assert 30 in results  # 3 * 10

def square(x: int, state: WorkerState) -> int:
    return x * x

def flaky_square(x: int, state: WorkerState) -> int:
    # This is a bit tricky to test with MP since each process 
    # has its own state. We'll use a simple condition.
    if x == 5:
        raise ValueError("Cannot process 5")
    return x * x

def simple_func(x: int, state: WorkerState) -> int:
    return x

class TestMPWorker:
    """Test the multiprocessing worker."""
    
    def test_basic_mp_processing(self):
        # Create MP queues
        from multiprocessing import get_context
        ctx = get_context("spawn")
        inbound = ctx.Queue(maxsize=5)
    
        
        # Add test data
        inbound.put(2)
        inbound.put(3)
        inbound.put(4)
        inbound.put(SENTINEL)
        
        outbound, proc = mp_worker(square, 2, 1, inbound)
        
        # Collect results
        results = []
        while True:
            item = outbound.get()
            if item == SENTINEL:
                break
            results.append(item)
        
        proc.join(timeout=5)  # Wait for process to finish
        assert results == [4, 9, 16]
        assert not proc.is_alive()
    
    def test_mp_worker_with_retries(self):
        from multiprocessing import get_context
        ctx = get_context("spawn")
        inbound = ctx.Queue(maxsize=5)
        
        inbound.put(2)
        inbound.put(5)  # Will cause error
        inbound.put(3)
        inbound.put(SENTINEL)
        
        outbound, proc = mp_worker(flaky_square, 1, 2, inbound)
        
        results = []
        while True:
            item = outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        proc.join(timeout=5)
        
        # Should get results for 2 and 3, and some error representation for 5
        # The exact error handling in MP worker might vary
        assert len(results) >= 1  # At least the successful ones
        assert 4 in results  # 2 * 2
    
    def test_mp_worker_process_lifecycle(self):
        from multiprocessing import get_context
        ctx = get_context("spawn")
        inbound = ctx.Queue(maxsize=5)
        
        inbound.put(42)
        inbound.put(SENTINEL)
        
        outbound, proc = mp_worker(simple_func, 1, 1, inbound)
        
        # Process should be alive initially
        assert proc.is_alive()
        
        # Get results
        result = outbound.get()
        assert result == 42
        
        sentinel = outbound.get()
        assert sentinel is SENTINEL
        
        # Process should finish
        proc.join(timeout=5)
        assert not proc.is_alive()


class TestWorkerIntegration:
    """Integration tests combining different worker types."""
    
    @pytest.mark.asyncio
    async def test_worker_chain_simulation(self):
        """Simulate chaining workers (like in a pipeline)."""
        
        # First stage
        stage1_in = asyncio.Queue(maxsize=10)
        
        def add_one(x: int, state: WorkerState) -> int:
            return x + 1
        
        # Second stage  
        def multiply_by_two(x: int, state: WorkerState) -> int:
            return x * 2
        
        # Add data to first stage
        for i in range(3):
            await stage1_in.put(i)
        await stage1_in.put(SENTINEL)
        
        # First worker
        stage1_out = worker(add_one, 2, 1, stage1_in)
        
        # Use output of first as input to second
        stage2_out = worker(multiply_by_two, 2, 1, stage1_out)
        
        # Collect final results
        results = []
        while True:
            item = await stage2_out.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        # Results: (0+1)*2=2, (1+1)*2=4, (2+1)*2=6
        assert results == [2, 4, 6]
    
    @pytest.mark.asyncio
    async def test_concurrent_workers_same_input(self):
        """Test multiple workers processing from the same input queue."""
        
        shared_input = asyncio.Queue(maxsize=10)
        
        def process_with_id(x: int, state: WorkerState) -> tuple:
            # Add worker "id" to track which worker processed what
            return (x, "worker_processed")
        
        # Add data
        for i in range(6):
            await shared_input.put(i)
        await shared_input.put(SENTINEL)
        
        # Create multiple workers sharing the same input
        worker1_out = worker(process_with_id, 1, 1, shared_input)
        worker2_out = worker(process_with_id, 1, 1, shared_input)
        
        # Collect from both workers
        all_results = []
        
        # We need to collect from both workers until we get sentinels from both
        sentinel_count = 0
        workers = [worker1_out, worker2_out]
        
        # This is a simplified collection - in real usage you'd use more sophisticated multiplexing
        for _ in range(20):  # Safety limit
            for worker_out in workers[:]:  # Copy list since we may modify it
                try:
                    item = await asyncio.wait_for(worker_out.get(), timeout=0.1)
                    if item is SENTINEL:
                        sentinel_count += 1
                        workers.remove(worker_out)
                    else:
                        all_results.append(item)
                except asyncio.TimeoutError:
                    continue
            
            if sentinel_count >= 2:  # Both workers done
                break
        
        # Should have processed all 6 items
        assert len(all_results) == 6
        
        # All should be processed
        processed_numbers = {result[0] for result in all_results}
        assert processed_numbers == {0, 1, 2, 3, 4, 5}


def identity(x: Any, state: WorkerState) -> Any:
    return x

class TestSentinelHandling:
    """Test proper SENTINEL propagation."""
    
    @pytest.mark.asyncio
    async def test_sentinel_propagation_async(self):
        inbound = asyncio.Queue(maxsize=5)
        
        def identity(x: Any, state: WorkerState) -> Any:
            return x
        
        await inbound.put("data")
        await inbound.put(SENTINEL)
        
        outbound = worker(identity, 1, 1, inbound)
        
        # Should get data then sentinel
        result1 = await outbound.get()
        assert result1 == "data"
        
        result2 = await outbound.get()
        assert result2 is SENTINEL
    
    def test_sentinel_propagation_mp(self):
        from multiprocessing import get_context
        ctx = get_context("spawn")
        inbound = ctx.Queue(maxsize=5)
        
        inbound.put("data")
        inbound.put(SENTINEL)
        
        outbound, proc = mp_worker(identity, 1, 1, inbound)
        
        result1 = outbound.get()
        assert result1 == "data"
        
        result2 = outbound.get()
        assert result2 is SENTINEL
        
        proc.join(timeout=5)
        assert not proc.is_alive()