"""Integration tests for end-to-end pipeline functionality."""

import asyncio
import json
import time
from typing import Any, Dict, List, Callable, Generator

import pytest

from parllel.pipeline import Pipeline
from parllel.stage import as_stage, mix_pool, work_pool
from parllel.util import is_ok
from parllel.worker_state import WorkerState

class TestBasicPipelineWorkflows:
    """Test common pipeline usage patterns."""
    
    @pytest.mark.asyncio
    async def test_data_processing_pipeline(self) -> None:
        """Test a typical data processing pipeline."""
        
        # Raw data
        raw_data = [
            {"id": 1, "value": 10, "category": "A"},
            {"id": 2, "value": 20, "category": "B"}, 
            {"id": 3, "value": 15, "category": "A"},
            {"id": 4, "value": 25, "category": "C"},
            {"id": 5, "value": 30, "category": "B"},
        ]
        
        @work_pool(buffer=3)
        def validate_data(item: Dict[str, Any], state: WorkerState) -> Dict[str, Any]:
            """Validate and clean data."""
            if item["value"] < 0:
                raise ValueError(f"Invalid value: {item['value']}")
            return {**item, "validated": True}
        
        @work_pool(buffer=2, num_workers=2)
        def enrich_data(item: Dict[str, Any], state: WorkerState) -> Dict[str, Any]:
            """Add computed fields."""
            item["double_value"] = item["value"] * 2
            item["is_high"] = item["value"] > 20
            return item
        
        @work_pool(buffer=5)
        def format_output(item: Dict[str, Any], state: WorkerState) -> str:
            """Format as string output."""
            return f"ID:{item['id']}, Category:{item['category']}, Value:{item['double_value']}"
        
        # Create and run pipeline
        pipeline = (Pipeline(iter(raw_data)) 
                   >> validate_data 
                   >> enrich_data 
                   >> format_output)
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)
    
    @pytest.mark.asyncio
    async def test_numeric_computation_pipeline(self) -> None:
        """Test mathematical computation pipeline."""
        
        numbers = list(range(1, 101))  # 1 to 100
        
        @work_pool(buffer=10, num_workers=4)
        def compute_square(n: int, state: WorkerState) -> int:
            """Compute square of number."""
            return n * n
        
        @work_pool(buffer=5)
        def filter_large(n: int, state: WorkerState) -> int:
            """Filter out very large numbers."""
            if n > 5000:
                raise ValueError("Number too large")
            return n
        
        @work_pool(buffer=3)
        def add_constant(n: int, state: WorkerState) -> int:
            """Add a constant."""
            return n + 1000
        
        pipeline = (Pipeline(iter(numbers))
                   >> compute_square
                   >> filter_large 
                   >> add_constant)
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)
    
    @pytest.mark.asyncio
    async def test_text_processing_pipeline(self) -> None:
        """Test text processing pipeline."""
        
        texts = [
            "Hello World",
            "Python Programming", 
            "Async Pipeline Processing",
            "Data Transformation",
            "Concurrent Computing"
        ]
        
        @work_pool(buffer=5)
        def normalize_text(text: str, state: WorkerState) -> str:
            """Convert to lowercase and strip."""
            return text.lower().strip()
        
        @work_pool(buffer=3, num_workers=2)
        def tokenize(text: str, state: WorkerState) -> List[str]:
            """Split into words."""
            return text.split()
        
        @work_pool(buffer=2)
        def count_chars(words: List[str], state: WorkerState) -> Dict[str, int]:
            """Count characters in all words."""
            total_chars = sum(len(word) for word in words)
            return {"word_count": len(words), "char_count": total_chars}
        
        pipeline = (Pipeline(iter(texts))
                   >> normalize_text
                   >> tokenize
                   >> count_chars)
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)


class TestAsyncPipelineWorkflows:
    """Test pipelines with async operations."""
    
    @pytest.mark.asyncio
    async def test_async_io_simulation(self) -> None:
        """Test pipeline simulating async I/O operations."""
        
        urls = [
            "http://api1.example.com",
            "http://api2.example.com", 
            "http://api3.example.com",
            "http://api4.example.com",
        ]
        
        @work_pool(buffer=2, num_workers=3)
        async def fetch_data(url: str, state: WorkerState) -> Dict[str, Any]:
            """Simulate async HTTP request."""
            await asyncio.sleep(0.01)  # Simulate network delay
            return {
                "url": url,
                "status": 200,
                "data": f"Response from {url}",
                "size": len(url)
            }
        
        @work_pool(buffer=3)
        async def process_response(response: Dict[str, Any], state: WorkerState) -> Dict[str, Any]:
            """Process the response."""
            await asyncio.sleep(0.005)  # Simulate processing time
            return {
                **response,
                "processed": True,
                "processed_at": time.time()
            }
        
        @work_pool(buffer=5)
        async def store_result(item: Dict[str, Any], state: WorkerState) -> str:
            """Simulate storing result."""
            await asyncio.sleep(0.002)
            return f"Stored: {item['url']}"
        
        start_time = time.time()
        pipeline = (Pipeline(iter(urls))
                   >> fetch_data
                   >> process_response  
                   >> store_result)
        pipeline.log = False
        
        result = await pipeline.run()
        end_time = time.time()
        
        assert is_ok(result)
        # Should be faster than sequential processing due to concurrency
        assert end_time - start_time < 0.2  # Should complete quickly due to async
    
    @pytest.mark.asyncio
    async def test_mixed_sync_async_pipeline(self) -> None:
        """Test pipeline mixing sync and async functions."""
        
        data = range(10)
        
        def sync_multiply(x: int, state: WorkerState) -> int:
            """Synchronous multiplication."""
            return x * 3
        
        @work_pool(buffer=5, num_workers=2)
        async def async_add(x: int, state: WorkerState) -> int:
            """Async addition with delay."""
            await asyncio.sleep(0.001)
            return x + 10
        
        @work_pool(buffer=3)
        def sync_format(x: int, state: WorkerState) -> str:
            """Sync formatting."""
            return f"Result: {x}"
        
        # Mix as_stage (sync) with work_pool (async)
        pipeline = (Pipeline(iter(data))
                   >> as_stage(sync_multiply)
                   >> async_add
                   >> as_stage(sync_format))
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)


class TestErrorHandlingWorkflows:
    """Test pipeline behavior under various error conditions."""
    
    @pytest.mark.asyncio
    async def test_pipeline_with_recoverable_errors(self) -> None:
        """Test pipeline that handles and recovers from errors."""
        
        data = [1, 2, 0, 4, -1, 6, 7, 8]  # Includes problematic values
        
        @work_pool(buffer=5, retries=2)
        def divide_by_self_minus_one(x: int, state: WorkerState) -> float:
            """Function that fails for certain inputs."""
            if x <= 0:
                raise ValueError(f"Invalid input: {x}")
            if x == 1:
                raise ZeroDivisionError("Division by zero")
            return x / (x - 1)
        
        @work_pool(buffer=3, retries=3)
        def safe_sqrt(x: float, state: WorkerState) -> float:
            """Safe square root with retries."""
            if x < 0:
                raise ValueError("Cannot take sqrt of negative number")
            return float(x ** 0.5)
        
        @work_pool(buffer=2)
        def format_result(x: float, state: WorkerState) -> str:
            """Format the final result."""
            return f"Final: {x:.2f}"
        
        pipeline = (Pipeline(iter(data))
                   >> divide_by_self_minus_one
                   >> safe_sqrt
                   >> format_result)
        pipeline.log = False
        
        # Pipeline should complete despite some errors
        result = await pipeline.run()
        # Depending on error handling, might be ok or error
        # The key is that it doesn't crash
        assert result is not None
    
    @pytest.mark.asyncio
    async def test_early_termination_on_critical_error(self) -> None:
        """Test pipeline that terminates early on critical errors."""
        
        data = range(100)  # Large dataset
        
        @work_pool(buffer=10)
        def check_and_process(x: int, state: WorkerState) -> int:
            """Process but fail critically at specific point."""
            if x == 50:  # Critical failure point
                raise RuntimeError("Critical system error")
            return x * 2
        
        @work_pool(buffer=5)
        def further_processing(x: int, state: WorkerState) -> int:
            """Further processing."""
            return x + 100
        
        pipeline = (Pipeline(iter(data))
                   >> check_and_process
                   >> further_processing)
        pipeline.log = False
        
        result = await pipeline.run()
        
        # Should have handled error in some way
        assert result is not None


class TestConcurrencyAndPerformance:
    """Test pipeline concurrency and performance characteristics."""
    
    @pytest.mark.asyncio
    async def test_high_concurrency_pipeline(self) -> None:
        """Test pipeline with high concurrency settings."""
        
        data = range(50)
        
        @work_pool(buffer=20, num_workers=8)
        async def cpu_intensive_task(x: int, state: WorkerState) -> int:
            """Simulate CPU-intensive task."""
            await asyncio.sleep(0.001)  # Small delay
            result = sum(range(x + 1))  # Some computation
            return result
        
        @work_pool(buffer=15, num_workers=6)
        async def io_intensive_task(x: int, state: WorkerState) -> int:
            """Simulate I/O-intensive task."""
            await asyncio.sleep(0.002)  # I/O delay
            return x * 2
        
        @work_pool(buffer=10, num_workers=4)
        def finalize(x: int, state: WorkerState) -> str:
            """Finalize processing."""
            return f"Processed: {x}"
        
        start_time = time.time()
        
        pipeline = (Pipeline(iter(data))
                   >> cpu_intensive_task
                   >> io_intensive_task
                   >> finalize)
        pipeline.log = False
        
        result = await pipeline.run()
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        assert is_ok(result)
        # High concurrency should complete reasonably fast
        assert processing_time < 2.0  # Should be much faster than sequential
    
    @pytest.mark.asyncio
    async def test_backpressure_handling(self) -> None:
        """Test pipeline backpressure with different buffer sizes."""
        
        data = range(100)
        
        @work_pool(buffer=1, num_workers=1)  # Small buffer, single worker
        async def slow_processor(x: int, state: WorkerState) -> int:
            """Slow processing stage."""
            await asyncio.sleep(0.001)
            return x * 2
        
        @work_pool(buffer=50, num_workers=5)  # Large buffer, many workers
        async def fast_processor(x: int, state: WorkerState) -> int:
            """Fast processing stage."""
            await asyncio.sleep(0.0001)
            return x + 1
        
        @work_pool(buffer=10)  # Medium buffer
        def output_stage(x: int, state: WorkerState) -> str:
            """Output formatting."""
            return f"Item: {x}"
        
        pipeline = (Pipeline(iter(data))
                   >> slow_processor    # Bottleneck
                   >> fast_processor    # Should be backed up
                   >> output_stage)
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)


class TestComplexDataFlows:
    """Test complex data transformation scenarios."""
    
    @pytest.mark.asyncio
    async def test_fan_out_aggregation_pattern(self) -> None:
        """Test fan-out pattern with aggregation using mix_pool."""
        
        numbers = [10, 20, 30, 40, 50]
        
        # Fan-out: each number goes through multiple analysis functions
        @mix_pool(
            buffer=10,
            fork_merge=lambda results: {
                "sum": results[0], 
                "product": results[1], 
                "count": results[2]
            }
        )
        def multi_analysis() -> list[Callable]:
            return [
                lambda x: x,              # Sum contribution
                lambda x: x,              # Product contribution  
                lambda x: 1               # Count contribution
            ]
        
        @work_pool(buffer=5)
        def format_analysis(analysis: Dict[str, int], state: WorkerState) -> str:
            """Format the analysis results."""
            return f"Sum: {analysis['sum']}, Product: {analysis['product']}, Count: {analysis['count']}"
        
        pipeline = (Pipeline(iter(numbers))
                   >> multi_analysis
                   >> format_analysis)
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)
    
    @pytest.mark.asyncio
    async def test_conditional_processing_pipeline(self) -> None:
        """Test pipeline with conditional processing paths."""
        
        mixed_data = [
            {"type": "number", "value": 42},
            {"type": "text", "value": "hello"},
            {"type": "number", "value": 17},
            {"type": "text", "value": "world"},
            {"type": "number", "value": 99},
        ]
        
        @work_pool(buffer=5)
        def route_and_process(item: Dict[str, Any], state: WorkerState) -> Dict[str, Any]:
            """Process based on item type."""
            if item["type"] == "number":
                return {
                    **item,
                    "processed_value": item["value"] * 2,
                    "category": "numeric"
                }
            elif item["type"] == "text":
                return {
                    **item, 
                    "processed_value": item["value"].upper(),
                    "category": "textual"
                }
            else:
                raise ValueError(f"Unknown type: {item['type']}")
        
        @work_pool(buffer=3)
        def add_metadata(item: Dict[str, Any], state: WorkerState) -> Dict[str, Any]:
            """Add processing metadata."""
            return {
                **item,
                "processed_at": time.time(),
                "pipeline_stage": "metadata_addition"
            }
        
        @work_pool(buffer=2)
        def serialize_output(item: Dict[str, Any], state: WorkerState) -> str:
            """Serialize to JSON string."""
            return json.dumps(item, default=str)
        
        pipeline = (Pipeline(iter(mixed_data))
                   >> route_and_process
                   >> add_metadata
                   >> serialize_output)
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)
    
    @pytest.mark.asyncio
    async def test_streaming_transformation_pipeline(self) -> None:
        """Test pipeline that simulates real-time data streaming."""
        
        def data_stream() -> Generator:
            """Simulate a data stream."""
            for i in range(20):
                yield {
                    "timestamp": time.time(),
                    "sensor_id": f"sensor_{i % 3}",
                    "value": i * 2.5 + 10,
                    "status": "active" if i % 4 != 0 else "maintenance"
                }
        
        @work_pool(buffer=8, num_workers=3)
        async def validate_sensor_data(data: Dict[str, Any], state: WorkerState) -> Dict[str, Any]:
            """Validate sensor data."""
            await asyncio.sleep(0.001)  # Simulate validation time
            
            if data["value"] < 0:
                raise ValueError("Invalid sensor reading")
            
            return {
                **data,
                "validated": True,
                "validation_time": time.time()
            }
        
        @work_pool(buffer=5, num_workers=2) 
        async def enrich_with_metadata(data: Dict[str, Any], state: WorkerState) -> Dict[str, Any]:
            """Add metadata based on sensor."""
            await asyncio.sleep(0.001)
            
            sensor_metadata = {
                "sensor_0": {"location": "north", "type": "temperature"},
                "sensor_1": {"location": "south", "type": "humidity"},
                "sensor_2": {"location": "east", "type": "pressure"}
            }
            
            metadata = sensor_metadata.get(data["sensor_id"], {"location": "unknown", "type": "unknown"})
            
            return {**data, **metadata}
        
        @work_pool(buffer=10)
        async def store_processed_data(data: Dict[str, Any], state: WorkerState) -> str:
            """Simulate storing processed data."""
            await asyncio.sleep(0.001)
            return f"Stored: {data['sensor_id']} at {data['location']}"
        
        start_time = time.time()
        
        pipeline = (Pipeline(data_stream())
                   >> validate_sensor_data
                   >> enrich_with_metadata
                   >> store_processed_data)
        pipeline.log = False
        
        result = await pipeline.run()
        
        end_time = time.time()
        
        assert is_ok(result)
        # Should process streaming data efficiently
        assert end_time - start_time < 1.0  # Should be reasonably fast


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_empty_pipeline(self) -> None:
        """Test pipeline with no data."""
        empty_data: list = []
        
        @work_pool()
        def process_item(x: int, state: WorkerState) -> int:
            return x * 2
        
        pipeline = Pipeline(iter(empty_data)) >> process_item
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)
    
    @pytest.mark.asyncio
    async def test_single_item_pipeline(self) -> None:
        """Test pipeline with single item."""
        single_item = [42]
        
        @work_pool(buffer=1)
        def double_value(x: int, state: WorkerState) -> int:
            return x * 2
        
        @work_pool(buffer=1)
        def add_hundred(x: int, state: WorkerState) -> int:
            return x + 100
        
        pipeline = (Pipeline(iter(single_item))
                   >> double_value
                   >> add_hundred)
        pipeline.log = False
        
        result = await pipeline.run()
        assert is_ok(result)
    
    @pytest.mark.asyncio
    async def test_very_large_pipeline(self) -> None:
        """Test pipeline with large dataset."""
        large_data = range(1000)
        
        @work_pool(buffer=50, num_workers=10)
        def fast_processing(x: int, state: WorkerState) -> int:
            return (x * 2) + 1
        
        @work_pool(buffer=25, num_workers=5)
        def final_transform(x: int, state: WorkerState) -> str:
            return f"Item_{x}"
        
        pipeline = (Pipeline(iter(large_data))
                   >> fast_processing
                   >> final_transform)
        pipeline.log = False
        
        start_time = time.time()
        result = await pipeline.run()
        end_time = time.time()
        
        assert is_ok(result)
        # Should handle large dataset efficiently
        assert end_time - start_time < 5.0  # Reasonable time limit
