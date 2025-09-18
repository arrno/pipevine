"""Tests for WorkerState persistence across handler calls."""

import asyncio
import pytest
import multiprocessing as mp
from typing import Any

from worker import worker, worker_no_buf, mp_worker
from async_util import SENTINEL
from worker_state import WorkerState, WorkerHandler


class TestWorkerStatePersistence:
    """Test WorkerState persistence across multiple handler calls."""
    
    @pytest.mark.asyncio
    async def test_state_persists_across_calls(self):
        """Test that state persists across multiple handler calls in the same worker."""
        inbound = asyncio.Queue(maxsize=10)
        call_count = 0
        
        def counter_handler(x: int, state: WorkerState) -> int:
            nonlocal call_count
            call_count += 1
            
            # Initialize counter if not present
            if 'counter' not in state.values:
                state.update(counter=0)
            
            # Increment counter
            current = state.get('counter')
            state.update(counter=current + 1)
            
            return state.get('counter')
        
        # Add test data
        for i in range(5):
            await inbound.put(i)
        await inbound.put(SENTINEL)
        
        outbound = worker_no_buf(counter_handler, 1, inbound)
        
        # Collect results
        results = []
        while True:
            item = await outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        # State should persist, so we get incrementing counters
        assert results == [1, 2, 3, 4, 5]
        assert call_count == 5

    @pytest.mark.asyncio 
    async def test_connection_reuse_pattern(self):
        """Test pattern of reusing connections/clients across calls."""
        inbound = asyncio.Queue(maxsize=10)
        connection_created_count = 0
        
        class MockConnection:
            def __init__(self):
                nonlocal connection_created_count
                connection_created_count += 1
                self.used_count = 0
                
            def process(self, data):
                self.used_count += 1
                return f"processed_{data}_count_{self.used_count}"
        
        def connection_handler(data: str, state: WorkerState) -> str:
            # Initialize connection if not present
            if 'connection' not in state.values:
                state.update(connection=MockConnection())
            
            conn = state.get('connection')
            return conn.process(data)
        
        # Add test data
        test_data = ["item1", "item2", "item3"]
        for item in test_data:
            await inbound.put(item)
        await inbound.put(SENTINEL)
        
        outbound = worker_no_buf(connection_handler, 1, inbound)
        
        # Collect results
        results = []
        while True:
            item = await outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        # Should only create one connection, but use it multiple times
        assert connection_created_count == 1
        assert results == [
            "processed_item1_count_1",
            "processed_item2_count_2", 
            "processed_item3_count_3"
        ]

    @pytest.mark.asyncio
    async def test_multiple_workers_separate_state(self):
        """Test that different workers maintain separate state."""
        inbound = asyncio.Queue(maxsize=20)
        
        def worker_id_handler(data: int, state: WorkerState) -> tuple[int, int]:
            # Each worker gets a unique ID based on first call
            if 'worker_id' not in state.values:
                state.update(worker_id=data)
            
            worker_id = state.get('worker_id')
            return (worker_id, data)
        
        # Add test data - each worker should process different items
        for i in range(10):
            await inbound.put(i)
        await inbound.put(SENTINEL)
        
        # Create multiple workers (num_workers=3)
        outbound = worker(worker_id_handler, 3, 1, inbound)
        
        # Collect results
        results = []
        while True:
            item = await outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        # Each worker should have maintained its own state
        # Results should show different worker IDs for different items
        assert len(results) == 10
        
        # Group by worker_id to see state separation
        worker_groups = {}
        for worker_id, data in results:
            if worker_id not in worker_groups:
                worker_groups[worker_id] = []
            worker_groups[worker_id].append(data)
        
        # Should have multiple workers with separate states
        assert len(worker_groups) > 1


class TestWorkerStateOperations:
    """Test WorkerState get/update operations."""
    
    def test_get_with_default(self):
        """Test WorkerState.get() with default values."""
        state = WorkerState({})
        
        # Get with default when key doesn't exist
        assert state.get('missing_key', 'default_value') == 'default_value'
        assert state.get('missing_key') is None
        
        # Get existing value
        state.values['existing_key'] = 'existing_value'
        assert state.get('existing_key', 'default_value') == 'existing_value'
    
    def test_update_values(self):
        """Test WorkerState.update() method."""
        state = WorkerState({})
        
        # Update with new values
        state.update(key1='value1', key2='value2')
        assert state.values == {'key1': 'value1', 'key2': 'value2'}
        
        # Update existing values
        state.update(key1='updated_value1', key3='value3')
        assert state.values == {
            'key1': 'updated_value1', 
            'key2': 'value2', 
            'key3': 'value3'
        }

    def test_complex_state_objects(self):
        """Test storing complex objects in state."""
        state = WorkerState({})
        
        # Store complex objects
        complex_obj = {'nested': {'data': [1, 2, 3]}, 'count': 42}
        state.update(complex=complex_obj)
        
        retrieved = state.get('complex')
        assert retrieved == complex_obj
        assert retrieved is complex_obj  # Same object reference


def process_with_state(data: int, state: WorkerState) -> tuple[int, int]:
    # Track process-local state
    if 'process_counter' not in state.values:
        state.update(process_counter=0)
    
    counter = state.get('process_counter') + 1
    state.update(process_counter=counter)
    
    return (data, counter)

# Simple connection-like class that can be pickled
class SimpleConnection:
    def __init__(self):
        self.created_at = mp.current_process().pid
        self.call_count = 0
    
    def process_data(self, data):
        self.call_count += 1
        return f"pid_{self.created_at}_call_{self.call_count}_data_{data}"

def handler_with_connection(data: str, state: WorkerState) -> str:
    # Create connection if not exists (should happen once per process)
    if 'connection' not in state.values:
        state.update(connection=SimpleConnection())
    
    conn = state.get('connection')
    return conn.process_data(data)

def state_modifier(data: int, state: WorkerState) -> int:
    # Modify shared state
    if 'shared_value' not in state.values:
        state.update(shared_value=0)
    
    current = state.get('shared_value')
    new_value = current + data
    state.update(shared_value=new_value)
    
    return new_value

class TestMultiProcessBoundaryScenarios:
    """Test WorkerState behavior across multi-process boundaries."""
    
    def test_state_isolation_multiprocess(self):
        """Test that each multiprocess worker maintains isolated state."""
        if mp.get_start_method() != 'spawn':
            pytest.skip("Multiprocessing tests require spawn start method")
            
        inbound = mp.Queue()
        
        # Add test data
        for i in range(5):
            inbound.put(i)
        inbound.put(SENTINEL)
        
        # Create multiprocess worker
        outbound, process = mp_worker(process_with_state, 2, 1, inbound)
        
        # Collect results
        results = []
        while True:
            try:
                item = outbound.get(timeout=5)
                if item is SENTINEL:
                    break
                results.append(item)
            except:
                break
        
        process.join(timeout=5)
        
        # Each item should increment the process-local counter
        assert len(results) == 5
        data_values = [data for data, counter in results]
        counter_values = [counter for data, counter in results]
        
        assert data_values == [0, 1, 2, 3, 4]
        assert counter_values == [1, 2, 3, 4, 5]  # Sequential counter
    
    def test_connection_pattern_multiprocess(self):
        """Test connection-like object creation in multiprocess worker."""
        if mp.get_start_method() != 'spawn':
            pytest.skip("Multiprocessing tests require spawn start method")
            
        inbound = mp.Queue()
        
        # Add test data
        test_items = ['item1', 'item2', 'item3']
        for item in test_items:
            inbound.put(item)
        inbound.put(SENTINEL)
        
        # Create multiprocess worker
        outbound, process = mp_worker(handler_with_connection, 2, 1, inbound)
        
        # Collect results
        results = []
        while True:
            try:
                item = outbound.get(timeout=5)
                if item is SENTINEL:
                    break
                results.append(item)
            except:
                break
        
        process.join(timeout=5)
        
        # Should create one connection per process and reuse it
        assert len(results) == 3
        
        # All results should have same PID but incrementing call count
        pids = set()
        for result in results:
            # Extract PID from result string
            pid_part = result.split('_')[1]
            pids.add(pid_part)
        
        # Should only have one unique PID (one process)
        assert len(pids) == 1
        
        # Call counts should increment
        call_counts = []
        for result in results:
            call_part = result.split('_')[3] 
            call_counts.append(int(call_part))
        
        assert call_counts == [1, 2, 3]

    def test_state_cannot_cross_process_boundary(self):
        """Test that state modifications in one process don't affect another."""
        if mp.get_start_method() != 'spawn':
            pytest.skip("Multiprocessing tests require spawn start method")
            
        # This test demonstrates that WorkerState is process-local
        # by showing that two separate mp_worker instances don't share state
        
        # Create two separate multiprocess workers
        inbound1 = mp.Queue()
        inbound2 = mp.Queue()
        
        # Add data to both
        inbound1.put(10)
        inbound1.put(SENTINEL)
        
        inbound2.put(100) 
        inbound2.put(SENTINEL)
        
        # Start both workers
        outbound1, process1 = mp_worker(state_modifier, 1, 1, inbound1)
        outbound2, process2 = mp_worker(state_modifier, 1, 1, inbound2)
        
        # Collect results
        result1 = outbound1.get(timeout=5)
        result2 = outbound2.get(timeout=5)
        
        process1.join(timeout=5)
        process2.join(timeout=5)
        
        # Each process should start with its own state
        # Process 1: 0 + 10 = 10
        # Process 2: 0 + 100 = 100 (not 10 + 100 = 110)
        assert result1 == 10
        assert result2 == 100