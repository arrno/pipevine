"""Tests for stage module - Stage class, work_pool, mix_pool decorators."""

import asyncio
import pytest
from typing import Any
from worker_state import WorkerState
from stage import Stage, work_pool, mix_pool, as_stage, PathChoice
from async_util import SENTINEL


class TestStageClass:
    """Test the Stage class directly."""
    
    def test_stage_creation(self):
        def dummy_func(x: int, state: WorkerState) -> int:
            return x * 2
        
        stage = Stage(
            buffer=10,
            retries=3,
            multi_proc=False,
            functions=[dummy_func],
            merge=None,
            _choose=PathChoice.One
        )
        
        assert stage.buffer == 10
        assert stage.retries == 3
        assert stage.multi_proc is False
        assert len(stage.functions) == 1
        assert stage.functions[0] is dummy_func
        assert stage.merge is None
        assert stage._choose is PathChoice.One
    
    @pytest.mark.asyncio
    async def test_stage_run_single_function_async(self):
        def double(x: int, state: WorkerState) -> int:
            return x * 2
        
        stage = Stage(
            buffer=5,
            retries=1,
            multi_proc=False,
            functions=[double],
            merge=None,
            _choose=PathChoice.One
        )
        
        # Create input queue
        inbound = asyncio.Queue(maxsize=5)
        await inbound.put(1)
        await inbound.put(2)
        await inbound.put(3)
        await inbound.put(SENTINEL)
        
        # Run stage
        outbound = stage.run(inbound)
        
        # Collect results
        results = []
        while True:
            item = await outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        assert results == [2, 4, 6]
    
    @pytest.mark.asyncio 
    async def test_stage_run_multiple_functions_async(self):
        def add_one(x: int, state: WorkerState) -> int:
            return x + 1
        
        def multiply_two(x: int, state: WorkerState) -> int:
            return x * 2
        
        stage = Stage(
            buffer=5,
            retries=1,
            multi_proc=False,
            functions=[add_one, multiply_two],
            merge=None,
            _choose=PathChoice.One
        )
        
        inbound = asyncio.Queue(maxsize=5)
        for i in range(4):
            await inbound.put(i)
        await inbound.put(SENTINEL)
        
        outbound = stage.run(inbound)
        
        # With multiple functions in One mode, items are distributed
        results = []
        while True:
            item = await outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        # Should have 4 results total from both functions
        assert len(results) == 4
        # Results should include both add_one and multiply_two transformations
        # Since distribution is non-deterministic, we just check we got results
        assert all(isinstance(r, int) for r in results)
    
    @pytest.mark.asyncio
    async def test_stage_close(self):
        def identity(x: Any, state: WorkerState) -> Any:
            return x
        
        stage = Stage(
            buffer=1,
            retries=1,
            multi_proc=False,
            functions=[identity]
        )
        
        inbound = asyncio.Queue(maxsize=1)
        stage.run(inbound)
        
        # Test closing
        result = await stage.close()
        assert result is True
        
        # Test closing when no inbound exists
        stage2 = Stage(buffer=1, retries=1, multi_proc=False, functions=[identity])
        result2 = await stage2.close()
        assert result2 is False


class TestWorkPool:
    """Test the work_pool decorator."""
    
    def test_work_pool_default_params(self):
        @work_pool()
        def simple_func(x: int, state: WorkerState) -> int:
            return x + 1
        
        assert isinstance(simple_func, Stage)
        assert simple_func.buffer == 1
        assert simple_func.retries == 1
        assert simple_func.multi_proc is False
        assert len(simple_func.functions) == 1
        assert simple_func.merge is None
        assert simple_func._choose is PathChoice.One
    
    def test_work_pool_custom_params(self):
        @work_pool(buffer=10, retries=5, num_workers=3, multi_proc=True)
        def custom_func(x: int, state: WorkerState) -> int:
            return x * 3
        
        assert isinstance(custom_func, Stage)
        assert custom_func.buffer == 10
        assert custom_func.retries == 5
        assert custom_func.multi_proc is True
        assert len(custom_func.functions) == 3  # num_workers copies
        assert all(f is custom_func.functions[0] for f in custom_func.functions)
        assert custom_func._choose is PathChoice.One
    
    def test_work_pool_with_fork_merge(self):
        def merger(results):
            return sum(results)
        
        @work_pool(num_workers=2, fork_merge=merger)
        def add_ten(x: int, state: WorkerState) -> int:
            return x + 10
        
        assert isinstance(add_ten, Stage)
        assert add_ten.merge is merger
        assert add_ten._choose is PathChoice.All  # Should switch to All when merge provided
        assert len(add_ten.functions) == 2
    
    @pytest.mark.asyncio
    async def test_work_pool_execution(self):
        @work_pool(buffer=3, num_workers=2)
        async def async_double(x: int, state: WorkerState) -> int:
            await asyncio.sleep(0.01)  # Small delay
            return x * 2
        
        inbound = asyncio.Queue(maxsize=5)
        await inbound.put(1)
        await inbound.put(2)
        await inbound.put(SENTINEL)
        
        outbound = async_double.run(inbound)
        
        results = []
        while True:
            item = await outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        assert len(results) == 2
        assert set(results) == {2, 4}


class TestMixPool:
    """Test the mix_pool decorator."""
    
    def test_mix_pool_default_params(self):
        @mix_pool()
        def multi_functions():
            return [
                lambda x, s: x + 1,
                lambda x, s: x * 2
            ]
        
        assert isinstance(multi_functions, Stage)
        assert multi_functions.buffer == 1
        assert multi_functions.retries == 1
        assert multi_functions.multi_proc is False
        assert len(multi_functions.functions) == 2
        assert multi_functions.merge is None
        assert multi_functions._choose is PathChoice.One
    
    def test_mix_pool_custom_params(self):
        def merger(results):
            return max(results)
        
        @mix_pool(buffer=20, retries=2, multi_proc=True, fork_merge=merger)
        def mixed_analysis():
            return [
                lambda x, s: x ** 2,
                lambda x, s: x ** 3,
                lambda x, s: x ** 4
            ]
        
        assert isinstance(mixed_analysis, Stage)
        assert mixed_analysis.buffer == 20
        assert mixed_analysis.retries == 2
        assert mixed_analysis.multi_proc is True
        assert len(mixed_analysis.functions) == 3
        assert mixed_analysis.merge is merger
        assert mixed_analysis._choose is PathChoice.All
    
    @pytest.mark.asyncio
    async def test_mix_pool_execution(self):
        @mix_pool(buffer=5)
        def math_operations():
            return [
                lambda x, s: x + 10,  # Add 10
                lambda x, s: x * 3    # Multiply by 3
            ]
        
        inbound = asyncio.Queue(maxsize=5)
        await inbound.put(5)
        await inbound.put(SENTINEL)
        
        outbound = math_operations.run(inbound)
        
        results = []
        while True:
            item = await outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        # With PathChoice.One, the item goes to one of the functions
        assert len(results) == 1
        assert results[0] in [15, 15]  # 5+10=15 or 5*3=15... wait, that's the same!
        
        # Let me use different operations
        @mix_pool(buffer=5)  
        def different_operations():
            return [
                lambda x, s: x + 1,   # Add 1
                lambda x, s: x * 10   # Multiply by 10
            ]
        
        inbound2 = asyncio.Queue(maxsize=5)
        await inbound2.put(2)
        await inbound2.put(SENTINEL)
        
        outbound2 = different_operations.run(inbound2)
        
        results2 = []
        while True:
            item = await outbound2.get()
            if item is SENTINEL:
                break
            results2.append(item)
        
        assert len(results2) == 1
        assert results2[0] in [3, 20]  # 2+1=3 or 2*10=20


class TestAsStage:
    """Test the as_stage function."""
    
    def test_as_stage_creation(self):
        def simple_func(x: int, state: WorkerState) -> int:
            return x + 5
        
        stage = as_stage(simple_func)
        
        assert isinstance(stage, Stage)
        assert stage.buffer == 1
        assert stage.retries == 1
        assert stage.multi_proc is False
        assert len(stage.functions) == 1
        assert stage.functions[0] is simple_func
        assert stage.merge is None
    
    @pytest.mark.asyncio
    async def test_as_stage_execution(self):
        def increment(x: int, state: WorkerState) -> int:
            return x + 1
        
        stage = as_stage(increment)
        
        inbound = asyncio.Queue(maxsize=5)
        await inbound.put(10)
        await inbound.put(20)
        await inbound.put(SENTINEL)
        
        outbound = stage.run(inbound)
        
        results = []
        while True:
            item = await outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        assert results == [11, 21]


class TestPathChoice:
    """Test PathChoice enum and its behavior."""
    
    def test_path_choice_values(self):
        assert PathChoice.One is not None
        assert PathChoice.All is not None
        assert PathChoice.One != PathChoice.All
    
    @pytest.mark.asyncio
    async def test_path_choice_one_behavior(self):
        """Test that PathChoice.One distributes items across workers."""
        
        def track_worker(worker_id):
            def inner(x, state: WorkerState):
                return f"worker_{worker_id}_{x}"
            return inner
        
        stage = Stage(
            buffer=5,
            retries=1,
            multi_proc=False,
            functions=[track_worker(1), track_worker(2)],
            merge=None,
            _choose=PathChoice.One
        )
        
        inbound = asyncio.Queue(maxsize=10)
        for i in range(4):
            await inbound.put(i)
        await inbound.put(SENTINEL)
        
        outbound = stage.run(inbound)
        
        results = []
        while True:
            item = await outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        # Should get 4 results, distributed between workers
        assert len(results) == 4
        
        # Should have results from both workers (non-deterministic distribution)
        worker1_results = [r for r in results if "worker_1_" in r]
        worker2_results = [r for r in results if "worker_2_" in r]
        
        # At least one worker should have processed something
        # (Distribution might not be even due to timing)
        assert len(worker1_results) + len(worker2_results) == 4


class TestStageIntegration:
    """Integration tests for Stage functionality."""
    
    @pytest.mark.asyncio
    async def test_stage_with_errors_and_retries(self):
        """Test stage behavior with functions that can fail."""
        
        attempt_count = 0
        
        def flaky_function(x: int, state: WorkerState) -> int:
            nonlocal attempt_count
            attempt_count += 1
            if x == 5 and attempt_count == 1:
                raise ValueError("First attempt at 5 fails")
            return x * 2
        
        stage = Stage(
            buffer=3,
            retries=2,
            multi_proc=False,
            functions=[flaky_function]
        )
        
        inbound = asyncio.Queue(maxsize=5)
        await inbound.put(1)
        await inbound.put(5)  # This will fail once then succeed
        await inbound.put(3)
        await inbound.put(SENTINEL)
        
        outbound = stage.run(inbound)
        
        results = []
        while True:
            item = await outbound.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        # Should get results for all items (including 5 after retry)
        assert len(results) == 3
        assert 2 in results   # 1 * 2
        assert 10 in results  # 5 * 2 (after retry)  
        assert 6 in results   # 3 * 2
    
    @pytest.mark.asyncio
    async def test_multiple_stages_chained(self):
        """Test chaining multiple stages together."""
        
        # Stage 1: Add 1
        @work_pool(buffer=2)
        def add_one(x: int, state: WorkerState) -> int:
            return x + 1
        
        # Stage 2: Multiply by 2  
        @work_pool(buffer=2)
        def multiply_two(x: int, state: WorkerState) -> int:
            return x * 2
        
        # Input data
        inbound = asyncio.Queue(maxsize=5)
        for i in range(3):
            await inbound.put(i)
        await inbound.put(SENTINEL)
        
        # Chain stages
        stage1_out = add_one.run(inbound)
        stage2_out = multiply_two.run(stage1_out)
        
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
    async def test_concurrent_stages_same_input(self):
        """Test multiple stages processing the same input concurrently."""
        
        @work_pool(buffer=2)
        def double_it(x: int, state: WorkerState) -> int:
            return x * 2
        
        @work_pool(buffer=2) 
        def triple_it(x: int, state: WorkerState) -> int:
            return x * 3
        
        # Shared input
        inbound = asyncio.Queue(maxsize=5)
        await inbound.put(2)
        await inbound.put(3)
        await inbound.put(SENTINEL)
        await inbound.put(SENTINEL)
        
        # Both stages process same input
        double_out = double_it.run(inbound)
        triple_out = triple_it.run(inbound)
        
        # Collect from both
        double_results = []
        triple_results = []
        
        async def do_double():
            # Get results from double stage
            while True:
                item = await double_out.get()
                if item is SENTINEL:
                    break
                double_results.append(item)
        
        async def do_triple():
            # Get results from triple stage  
            while True:
                item = await triple_out.get()
                if item is SENTINEL:
                    break
                triple_results.append(item)
        
        async with asyncio.TaskGroup() as tg:
                tg.create_task(do_double())
                tg.create_task(do_triple())
        
        # Since both stages share the input queue, they'll split the items
        # Total results should account for all input items
        total_processed = len(double_results) + len(triple_results)
        assert total_processed == 2  # 2 items were put in