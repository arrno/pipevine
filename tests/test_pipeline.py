"""Tests for pipeline module - Pipeline class and composition."""

import asyncio
from asyncio import Event
from typing import Any, Iterator, Generator, Callable, AsyncIterator
from unittest.mock import patch

import pytest

from pipevine.async_util import SENTINEL
from pipevine.pipeline import Pipeline, PipelineMetrics
from pipevine.stage import Stage, mix_pool, work_pool, KillSwitch
from pipevine.util import Err, get_err, is_err, is_ok
from pipevine.worker_state import WorkerState


def _metric_mp_double(x: int, state: WorkerState) -> int:
    return x * 2


def _metric_mp_increment(x: int, state: WorkerState) -> int:
    return x + 1


class TestPipelineCreation:
    """Test Pipeline creation and basic setup."""
    
    def test_pipeline_creation_with_generator(self) -> None:
        data = [1, 2, 3, 4, 5]
        pipeline = Pipeline(iter(data), True)
        
        assert pipeline.generator is not None
        assert pipeline.stages == []
        assert pipeline.log is True
        assert is_ok(pipeline.result)
        assert isinstance(pipeline.result, PipelineMetrics)
    
    def test_pipeline_creation_empty(self) -> None:
        empty_gen: Iterator[Any] = iter([])
        pipeline = Pipeline(empty_gen)
        
        assert pipeline.generator is not None
        assert pipeline.stages == []
    
    def test_pipeline_gen_method(self) -> None:
        pipeline = Pipeline(iter([]))
        new_data = [10, 20, 30]
        
        result = pipeline.gen(iter(new_data))
        
        assert result is pipeline  # Should return self for chaining
        assert pipeline.generator is not None


class TestPipelineStageManagement:
    """Test adding and managing stages in the pipeline."""
    
    def test_stage_method(self) -> None:
        @work_pool()
        def double(x: int, state: WorkerState) -> int:
            return x * 2
        
        pipeline = Pipeline(iter([1, 2, 3]))
        result = pipeline.stage(double)
        
        assert result is pipeline  # Should return self for chaining
        assert len(pipeline.stages) == 1
        assert pipeline.stages[0] is double
    
    def test_stage_method_with_empty_functions(self) -> None:
        # Create a stage with empty functions list
        empty_stage = Stage(1, 1, False, [], None)
        
        pipeline = Pipeline(iter([1, 2, 3]))
        result = pipeline.stage(empty_stage)
        
        assert result is pipeline
        assert len(pipeline.stages) == 0  # Should not add empty stage
    
    def test_multiple_stages(self) -> None:
        @work_pool()
        def add_one(x: int, state: WorkerState) -> int:
            return x + 1
        
        @work_pool() 
        def multiply_two(x: int, state: WorkerState) -> int:
            return x * 2
        
        pipeline = Pipeline(iter([1, 2, 3]))
        result = pipeline.stage(add_one).stage(multiply_two)
        
        assert result is pipeline
        assert len(pipeline.stages) == 2
        assert pipeline.stages[0] is add_one
        assert pipeline.stages[1] is multiply_two
    
    def test_rshift_operator(self) -> None:
        @work_pool()
        def increment(x: int, state: WorkerState) -> int:
            return x + 1
        
        pipeline = Pipeline(iter([1, 2, 3]))
        result = pipeline >> increment
        
        assert result is pipeline
        assert len(pipeline.stages) == 1
        assert pipeline.stages[0] is increment
    
    def test_chained_rshift_operators(self) -> None:
        @work_pool()
        def add_one(x: int, state: WorkerState) -> int:
            return x + 1
        
        @work_pool()
        def multiply_three(x: int, state: WorkerState) -> int:
            return x * 3
        
        data = [1, 2, 3]
        pipeline = Pipeline(iter(data)) >> add_one >> multiply_three
        
        assert len(pipeline.stages) == 2
        assert pipeline.stages[0] is add_one
        assert pipeline.stages[1] is multiply_three


class TestPipelineExecution:
    """Test pipeline execution and data flow."""
    
    @pytest.mark.asyncio
    async def test_simple_pipeline_execution(self) -> None:
        @work_pool()
        def double(x: int, state: WorkerState) -> int:
            return x * 2
        
        data = [1, 2, 3]
        pipeline = Pipeline(iter(data)) >> double
        
        # Disable logging for test
        pipeline.log = False
        
        result = await pipeline.run()
        
        assert is_ok(result)

    @pytest.mark.asyncio
    async def test_pipeline_accepts_async_iterator(self) -> None:
        async def async_source() -> AsyncIterator[int]:
            for value in [1, 2, 3]:
                yield value

        pipeline = Pipeline(async_source())
        pipeline.log = False

        result = await pipeline.run()

        assert is_ok(result)
    
    @pytest.mark.asyncio
    async def test_multi_stage_pipeline(self) -> None:
        @work_pool()
        def add_one(x: int, state: WorkerState) -> int:
            return x + 1
        
        @work_pool()
        def multiply_two(x: int, state: WorkerState) -> int:
            return x * 2
        
        data = [1, 2, 3]
        pipeline = Pipeline(iter(data)) >> add_one >> multiply_two
        pipeline.log = False
        
        result = await pipeline.run()
        
        assert is_ok(result)
        # Results would be: (1+1)*2=4, (2+1)*2=6, (3+1)*2=8
        # But we don't capture the final output in this test

    @pytest.mark.asyncio
    async def test_pipeline_metrics_success(self) -> None:
        @work_pool()
        def double(x: int, state: WorkerState) -> int:
            return x * 2

        data = [1, 2, 3]
        pipeline = Pipeline(iter(data)) >> double
        pipeline.log = False

        result = await pipeline.run()

        assert isinstance(result, PipelineMetrics)
        assert result is pipeline.metrics
        assert result.processed == 3
        assert result.failed == 0
        assert len(result.stages) == 1
        stage_metrics = result.stages[0]
        assert stage_metrics.processed == 3
        assert stage_metrics.failed == 0
        assert stage_metrics.start > 0
        assert stage_metrics.stop >= stage_metrics.start
        assert stage_metrics.duration >= 0
        assert result.start > 0
        assert result.stop >= result.start
        assert result.duration >= 0

    @pytest.mark.asyncio
    async def test_pipeline_metrics_counts_failures(self) -> None:
        @work_pool(retries=1)
        def flaky(x: int, state: WorkerState) -> int:
            if x == 1:
                raise ValueError("nope")
            return x

        data = [1, 2, 3]
        pipeline = Pipeline(iter(data)) >> flaky
        pipeline.log = False

        result = await pipeline.run()

        assert isinstance(result, PipelineMetrics)
        assert result.failed == 1
        assert result.processed == 2
        assert len(result.stages) == 1
        stage_metrics = result.stages[0]
        assert stage_metrics.failed == 1
        assert stage_metrics.processed == 2

    @pytest.mark.asyncio
    async def test_pipeline_metrics_multi_worker_async_and_mp(self) -> None:
        @work_pool(buffer=3, num_workers=2)
        async def async_add_one(x: int, state: WorkerState) -> int:
            await asyncio.sleep(0)
            return x + 1

        mp_stage = work_pool(buffer=4, num_workers=2, multi_proc=True)(_metric_mp_double)

        data = [1, 2, 3, 4]
        pipeline = Pipeline(iter(data)) >> async_add_one >> mp_stage
        pipeline.log = False

        result = await pipeline.run()

        assert isinstance(result, PipelineMetrics)
        assert len(result.stages) == 2
        first_stage, second_stage = result.stages
        assert first_stage.processed == len(data)
        assert second_stage.processed == len(data)
        assert first_stage.failed == 0
        assert second_stage.failed == 0

    @pytest.mark.asyncio
    async def test_pipeline_metrics_retry_success_not_failure(self) -> None:
        @work_pool(retries=3)
        def flaky_retry(x: int, state: WorkerState) -> int:
            attempts = state.get("attempts", {})
            count = attempts.get(x, 0) + 1
            attempts[x] = count
            state.update(attempts=attempts)
            if count == 1:
                raise ValueError("fail first")
            return x

        mp_stage = work_pool(buffer=2, num_workers=2, multi_proc=True)(_metric_mp_increment)

        data = [1, 2, 3]
        pipeline = Pipeline(iter(data)) >> flaky_retry >> mp_stage
        pipeline.log = False

        result = await pipeline.run()

        assert isinstance(result, PipelineMetrics)
        assert result.failed == 0
        assert len(result.stages) == 2
        retry_stage, mp_metrics = result.stages
        assert retry_stage.failed == 0
        assert retry_stage.processed == len(data)
        assert mp_metrics.failed == 0
        assert mp_metrics.processed == len(data)

    @pytest.mark.asyncio
    async def test_multi_stage_pipeline_num_workers(self) -> None:
        @work_pool(num_workers=2)
        def add_one(x: int, state: WorkerState) -> int:
            return x + 1

        @work_pool(num_workers=3)
        def multiply_two(x: int, state: WorkerState) -> int:
            return x * 2
        
        data = [1, 2, 3]
        pipeline = Pipeline(iter(data)) >> add_one >> multiply_two
        pipeline.log = False
        
        result = await pipeline.run()
        
        assert is_ok(result)
        # Results would be: (1+1)*2=4, (2+1)*2=6, (3+1)*2=8
        # But we don't capture the final output in this test

    @pytest.mark.asyncio
    async def test_multi_stage_mp_pipeline(self) -> None:
        @work_pool(num_workers=2, multi_proc=True)
        def add_one(x: int, state: WorkerState) -> int:
            return x + 1

        @work_pool(num_workers=3, multi_proc=True)
        def multiply_two(x: int, state: WorkerState) -> int:
            return x * 2

        data = range(10)
        pipeline = Pipeline(iter(data)) >> add_one >> multiply_two
        pipeline.log = False
        
        result = await pipeline.run()
        
        assert is_ok(result)

    @pytest.mark.asyncio
    async def test_pipeline_with_no_generator(self) -> None:
        @work_pool()
        def identity(x: Any, state: WorkerState) -> Any:
            return x
        
        pipeline = Pipeline(iter([]))
        pipeline.generator = None  # Simulate no generator
        _ = pipeline >> identity
        
        result = await pipeline.run()
        
        assert is_err(result)


class TestPipelineCancellation:
    """Validate cooperative cancellation behaviour."""

    @pytest.mark.asyncio
    async def test_cancel_mid_run(self) -> None:
        data = list(range(50))
        pause_event = Event()

        @work_pool(buffer=2)
        async def slow_double(x: int, state: WorkerState) -> int:
            if x > 10:
                pause_event.set()
                await asyncio.sleep(3600)
            return x * 2
        
        pipeline = Pipeline(iter(data)) >> slow_double
        pipeline.log = False

        run_task = asyncio.create_task(pipeline.run())
        await pause_event.wait()

        cancel_result = await pipeline.cancel("stop")
        run_result = await asyncio.wait_for(run_task, timeout=2)

        assert is_err(cancel_result)
        assert is_err(run_result)
        assert get_err(run_result) == "stop"

    @pytest.mark.asyncio
    async def test_cancel_from_stage(self) -> None:
        data = list(range(50))

        @work_pool(buffer=2)
        async def double(x: int, state: WorkerState) -> int | KillSwitch:
            if x > 10:
                return KillSwitch("too large")
            return x * 2
        
        pipeline = Pipeline(iter(data)) >> double
        pipeline.log = False

        run_task = asyncio.create_task(pipeline.run())
        run_result = await asyncio.wait_for(run_task, timeout=2)

        assert is_err(run_result)
        assert get_err(run_result) == "too large"

    @pytest.mark.asyncio
    async def test_cancel_before_run_returns_error(self) -> None:
        pipeline = Pipeline(iter([1, 2, 3]))
        err = await pipeline.cancel("early")
        assert is_err(err)

        result = await pipeline.run()
        assert is_err(result)
        assert get_err(result) == "early"

    @pytest.mark.asyncio
    async def test_reseed_after_cancel(self) -> None:
        data = [1, 2, 3]

        pipeline = Pipeline(iter(data))
        await pipeline.cancel("halt")

        pipeline.gen(iter(data))

        result = await pipeline.run()
        assert is_ok(result)

    @pytest.mark.asyncio 
    async def test_pipeline_with_no_stages(self) -> None:
        data = [1, 2, 3, 4, 5]
        pipeline = Pipeline(iter(data))
        pipeline.log = False
        
        result = await pipeline.run()
        
        # Should complete successfully even with no stages
        assert is_ok(result)
    
    @pytest.mark.asyncio
    async def test_pipeline_error_handling_in_generator(self) -> None:
        def failing_generator() -> Generator:
            yield 1
            yield 2
            raise ValueError("Generator failed")
            # yield 3  # This won't be reached
        
        @work_pool()
        def identity(x: Any, state: WorkerState) -> Any:
            return x
        
        pipeline = Pipeline(failing_generator()) >> identity
        pipeline.log = False
        
        result = await pipeline.run()
        
        assert is_err(result)
        assert "Generator failed" in get_err(result)

class TestPipelineErrorHandling:
    """Test pipeline error handling and logging."""
    
    @pytest.mark.asyncio
    async def test_pipeline_handles_generator_errors(self) -> None:
        """Test that pipeline properly handles errors from generators."""
        def error_generator() -> Generator:
            yield 1
            yield 2
            raise ValueError("Generator error")
        
        @work_pool()
        def identity(x: int, state: WorkerState) -> int:
            return x
        
        pipeline = Pipeline(error_generator()) >> identity
        pipeline.log = False
        
        result = await pipeline.run()
        
        # Pipeline should handle the error
        assert is_err(result)
        assert "Generator error" in get_err(result)
    
    @pytest.mark.asyncio
    async def test_pipeline_logging_enabled(self) -> None:
        """Test pipeline with logging enabled."""
        data = [1, 2, 3]
        
        @work_pool()
        def identity(x: int, state: WorkerState) -> int:
            return x
        
        pipeline = Pipeline(iter(data)) >> identity
        pipeline.log = True
        pipeline._log_emit = True
        
        with patch('pipevine.pipeline.logger.info') as mock_print:
            result = await pipeline.run()
            
        # Should have printed the output items
        assert mock_print.called
        assert is_ok(result)
    
    @pytest.mark.asyncio
    async def test_pipeline_logging_disabled(self) -> None:
        """Test pipeline with logging disabled."""
        data = [1, 2, 3]
        
        @work_pool()
        def identity(x: int, state: WorkerState) -> int:
            return x
        
        pipeline = Pipeline(iter(data)) >> identity
        pipeline.log = False
        
        with patch('builtins.print') as mock_print:
            result = await pipeline.run()
        
        # Should not have printed anything
        assert not mock_print.called
        assert is_ok(result)


class TestPipelineDataHandling:
    """Test pipeline data handling and edge cases."""
    
    @pytest.mark.asyncio
    async def test_pipeline_with_normal_data_flow(self) -> None:
        """Test pipeline processes normal data correctly."""
        data = [1, 2, 3, 4, 5]
        
        @work_pool()
        def identity(x: int, state: WorkerState) -> int:
            return x
        
        pipeline = Pipeline(iter(data)) >> identity
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)

    
    @pytest.mark.asyncio
    async def test_pipeline_with_iterator_exception(self) -> None:
        """Test pipeline behavior when input iterator raises exceptions."""
        def failing_iterator() -> Generator:
            yield 1
            yield 2
            raise RuntimeError("Iterator failure")
            # yield 3  # Never reached
        
        @work_pool()
        def identity(x: int, state: WorkerState) -> int:
            return x
        
        pipeline = Pipeline(failing_iterator()) >> identity
        pipeline.log = False
        
        result = await pipeline.run()
        
        assert is_err(result)
        assert "Iterator failure" in get_err(result)


class TestPipelineIntegration:
    """Integration tests combining multiple pipeline features."""
    
    @pytest.mark.asyncio
    async def test_complex_pipeline_with_different_stage_types(self) -> None:
        """Test pipeline with various stage types and configurations."""
        
        @work_pool(buffer=5, num_workers=2)
        def preprocess(x: int, state: WorkerState) -> int:
            return x + 1
        
        @mix_pool(buffer=3, fork_merge=lambda results: sum(results))
        def analyze() -> list[Callable]:
            return [
                lambda x, s: x * 2,  # Double
                lambda x, s: x * 3   # Triple  
            ]
        
        @work_pool(retries=2)
        def postprocess(x: int, state: WorkerState) -> int:
            return x // 2  # Integer division
        
        data = [1, 2, 3]
        pipeline = Pipeline(iter(data)) >> preprocess >> analyze >> postprocess
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)
    
    @pytest.mark.asyncio
    async def test_pipeline_with_async_stages(self) -> None:
        """Test pipeline with async stage functions."""
        
        @work_pool(buffer=3)
        async def async_increment(x: int, state: WorkerState) -> int:
            await asyncio.sleep(0.001)  # Small async delay
            return x + 1
        
        @work_pool(buffer=2)
        async def async_double(x: int, state: WorkerState) -> int:
            await asyncio.sleep(0.001)
            return x * 2
        
        data = range(5)
        pipeline = Pipeline(iter(data)) >> async_increment >> async_double
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)
    
    @pytest.mark.asyncio
    async def test_pipeline_resilience_to_stage_errors(self) -> None:
        """Test pipeline behavior when stages have errors."""
        
        @work_pool(retries=3)
        def sometimes_fails(x: int, state: WorkerState) -> int:
            if x == 3:
                raise ValueError("Cannot process 3")
            return x * 10
        
        @work_pool()
        def final_stage(x: int, state: WorkerState) -> int:
            return x + 100
        
        data = [1, 2, 3, 4, 5]
        pipeline = Pipeline(iter(data)) >> sometimes_fails >> final_stage
        pipeline.log = False
        
        result = await pipeline.run()
        
        # Pipeline should complete, though item 3 might be handled as error
        assert is_ok(result)
    
    @pytest.mark.asyncio
    async def test_empty_data_pipeline(self) -> None:
        """Test pipeline behavior with empty input data."""
        
        @work_pool()
        def process_item(x: int, state: WorkerState) -> int:
            return x * 2
        
        empty_data: list = []
        pipeline = Pipeline(iter(empty_data)) >> process_item
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)
    
    @pytest.mark.asyncio
    async def test_large_data_pipeline(self) -> None:
        """Test pipeline with larger dataset."""
        
        @work_pool(buffer=10, num_workers=3)
        def fast_process(x: int, state: WorkerState) -> int:
            return x + 1
        
        @work_pool(buffer=5)
        def final_transform(x: int, state: WorkerState) -> int:
            return x * 2
        
        large_data = range(100)
        pipeline = Pipeline(iter(large_data)) >> fast_process >> final_transform
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)


class TestPipelineChaining:
    """Test various ways to chain pipeline operations."""
    
    @pytest.mark.asyncio
    async def test_method_chaining(self) -> None:
        """Test building pipeline with method chaining."""
        
        @work_pool()
        def stage1(x: int, state: WorkerState) -> int:
            return x + 1
        
        @work_pool()
        def stage2(x: int, state: WorkerState) -> int:
            return x * 2
        
        @work_pool()
        def stage3(x: int, state: WorkerState) -> int:
            return x - 1
        
        data = [1, 2, 3]
        pipeline = (Pipeline(iter(data))
                   .stage(stage1)
                   .stage(stage2) 
                   .stage(stage3))
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)
        assert len(pipeline.stages) == 3
    
    @pytest.mark.asyncio
    async def test_operator_chaining(self) -> None:
        """Test building pipeline with >> operators."""
        
        @work_pool()
        def stage1(x: int, state: WorkerState) -> int:
            return x + 5
        
        @work_pool() 
        def stage2(x: int, state: WorkerState) -> int:
            return x * 3
        
        data = [1, 2, 3]
        pipeline = Pipeline(iter(data)) >> stage1 >> stage2
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)
        assert len(pipeline.stages) == 2
    
    @pytest.mark.asyncio
    async def test_mixed_chaining_styles(self) -> None:
        """Test mixing method chaining and operator chaining."""
        
        @work_pool()
        def stage1(x: int, state: WorkerState) -> int:
            return x + 1
        
        @work_pool()
        def stage2(x: int, state: WorkerState) -> int:  
            return x * 2
        
        @work_pool()
        def stage3(x: int, state: WorkerState) -> int:
            return x + 10
        
        data = [1, 2, 3]
        pipeline = ((Pipeline(iter(data))
                   .stage(stage1) 
                   >> stage2)
                   .stage(stage3))
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)
        assert len(pipeline.stages) == 3

    @pytest.mark.asyncio
    async def test_pipeline_iter(self) -> None:
        """Test mixing method chaining stages and pipelines."""

        results = []
        
        @work_pool()
        def stage1(x: int, state: WorkerState) -> int:
            return x + 1
        
        @work_pool()
        def stage2(x: int, state: WorkerState) -> int:  
            return x * 2
        
        @work_pool()
        def stage3(x: int, state: WorkerState) -> int:
            return x + 10
        
        data = [1, 2, 3]
        expected = [14, 16, 18]

        pipe = (
            Pipeline(iter(data)) >>
            stage1 >>
            stage2 >>
            stage3
        )
        
        for item in pipe.iter():
            results.append(item)

        assert results == expected


    @pytest.mark.asyncio
    async def test_chaining_pipelines(self) -> None:
        """Test mixing method chaining stages and pipelines."""

        results = []
        
        @work_pool()
        def stage1(x: int, state: WorkerState) -> int:
            return x + 1
        
        @work_pool()
        def stage2(x: int, state: WorkerState) -> int:  
            return x * 2
        
        @work_pool()
        def stage3(x: int, state: WorkerState) -> int:
            results.append(x + 10)
            return x + 10
        
        data = [1, 2, 3]
        expected = [14, 16, 18]
        
        result = await (
            Pipeline(iter(data)) >> 
            stage1 >> 
            stage2 >> 
            (
                Pipeline(iter([])) >> 
                stage3
            )
        ).run()

        assert is_ok(result)
        assert results == expected

        results = []
        result = await (
            Pipeline(
                Pipeline(iter(data)) >> 
                stage1 >> 
                stage2
            ) >> 
            stage3
        ).run()

        assert is_ok(result)
        assert results == expected
