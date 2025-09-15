# Pypline

A high-performance async pipeline processing library for Python that enables efficient, concurrent data processing with backpressure control and automatic error handling.

## Features

-   **Async-first design** with optional multiprocessing support
-   **Backpressure control** via configurable buffering
-   **Automatic retry logic** with configurable retry counts
-   **Flexible worker patterns**: pool identical workers or mix different functions
-   **Pipeline composition** with method chaining or operator overloading
-   **Error handling** with Result types and graceful degradation

## Installation

```bash
pip install pypline
```

## Quick Start

```python
import asyncio
from pypline import Pipeline, work_pool

@work_pool(buffer=10, retries=3, num_workers=4)
async def process_data(item):
    # Your processing logic here
    return item * 2

@work_pool(buffer=5, retries=1)
async def validate_data(item):
    if item < 0:
        raise ValueError("Negative values not allowed")
    return item

# Create and run pipeline
pipeline = Pipeline(range(100)) >> process_data >> validate_data
result = await pipeline.run()
```

## Core Concepts

### Stages

Stages are the building blocks of pypline pipelines. Each stage processes data through one or more worker functions with configurable concurrency and error handling.

#### Work Pool (`@work_pool`)

Creates a stage with multiple identical workers processing items from a shared queue:

```python
@work_pool(
    buffer=10,        # Input queue buffer size for backpressure
    retries=3,        # Retry attempts on failure
    num_workers=4,    # Number of concurrent workers
    multi_proc=False, # Use multiprocessing instead of async
    fork_merge=None   # Optional: broadcast to all workers and merge results
)
async def my_stage(item):
    return process_item(item)
```

#### Mix Pool (`@mix_pool`)

Creates a stage with different worker functions, useful for heterogeneous processing:

```python
@mix_pool(
    buffer=20,
    multi_proc=True,
    fork_merge=lambda results: max(results)  # Merge results from all workers
)
def analysis_stage():
    return [
        lambda x: analyze_sentiment(x),
        lambda x: extract_keywords(x),
        lambda x: classify_topic(x)
    ]
```

### Pipeline Composition

#### Method Chaining

```python
pipeline = (Pipeline(data_source)
    .stage(preprocessing_stage)
    .stage(analysis_stage)
    .stage(output_stage))

result = await pipeline.run()
```

#### Operator Overloading

```python
pipeline = Pipeline(data_source) >> preprocessing >> analysis >> output
result = await pipeline.run()
```

## Configuration Options

### Stage Parameters

-   **`buffer`**: Input queue buffer size. Controls backpressure - higher values allow more items to queue but use more memory.
-   **`retries`**: Number of total attempts when a worker function raises an exception.
-   **`num_workers`** (work_pool only): Number of concurrent workers processing items.
-   **`multi_proc`**: When `True`, uses multiprocessing for CPU-bound tasks. When `False` (default), uses async/await for I/O-bound tasks.
-   **`fork_merge`**: Optional merge function. When provided, each item is sent to ALL workers and results are merged using this function.

### Processing Modes

#### Pool Mode (default)

Items are distributed across workers (load balancing):

```python
@work_pool(num_workers=4)  # Items distributed across 4 workers
async def process(item):
    return heavy_computation(item)
```

#### Fork Mode

Items are broadcast to all workers, results are merged:

```python
@work_pool(num_workers=3, fork_merge=lambda results: sum(results))
async def aggregate(item):
    return analyze_aspect(item)  # Each worker analyzes different aspect
```

## Advanced Examples

### CPU-Intensive Processing

```python
@work_pool(multi_proc=True, num_workers=8, buffer=50)
def cpu_intensive(data):
    # CPU-bound work runs in separate processes
    return complex_calculation(data)
```

### I/O-Bound Processing with Retry Logic

```python
@work_pool(retries=5, num_workers=10, buffer=100)
async def fetch_data(url):
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()
```

### Multi-Stage Data Pipeline

```python
import asyncio
from pypline import Pipeline, work_pool, mix_pool

# Data ingestion stage
@work_pool(buffer=50, num_workers=2)
async def ingest(source):
    return await load_data(source)

# Parallel analysis stage
@mix_pool(fork_merge=lambda results: {**results[0], **results[1]})
def analyze():
    return [
        lambda item: {"sentiment": analyze_sentiment(item)},
        lambda item: {"keywords": extract_keywords(item)}
    ]

# Output stage
@work_pool(buffer=10, retries=2)
async def store(enriched_item):
    await database.store(enriched_item)
    return enriched_item

# Compose and run pipeline
async def main():
    data_sources = ["file1.json", "file2.json", "api_endpoint"]

    pipeline = (Pipeline(data_sources)
        .stage(ingest)
        .stage(analyze)
        .stage(store))

    result = await pipeline.run()
    return result

if __name__ == "__main__":
    asyncio.run(main())
```

## Error Handling

Pypline uses Result types for robust error handling:

```python
from pypline.util import Result, is_err, unwrap

@work_pool(retries=3)
async def might_fail(item):
    if should_fail(item):
        raise ValueError("Processing failed")
    return item * 2

# Pipeline automatically handles errors and retries
pipeline = Pipeline(data) >> might_fail
result = await pipeline.run()

if is_err(result):
    print(f"Pipeline failed: {result}")
else:
    print("Pipeline completed successfully")
```

## Performance Tips

1. **Buffer sizing**: Set buffer sizes based on your memory constraints and processing speed differences between stages.

2. **Worker count**: For I/O-bound tasks, use more workers than CPU cores. For CPU-bound tasks, match worker count to CPU cores.

3. **Multiprocessing**: Use `multi_proc=True` for CPU-intensive tasks, `multi_proc=False` for I/O-bound tasks.

4. **Backpressure**: Smaller buffers provide better backpressure control but may reduce throughput.

## Requirements

-   Python 3.10+
-   No external dependencies (uses only Python standard library)

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
