"""Tests for async_util module - async/MP queue bridging and multiplexing utilities."""

import asyncio
from asyncio import Queue
from multiprocessing import get_context
from multiprocessing.queues import Queue as MPQueue
from typing import List, Any

import pytest

from pipevine.async_util import (
    SENTINEL,
    async_to_mp_queue,
    async_to_mp_queue_with_ready,
    make_broadcast_inbounds,
    make_shared_inbound_for_pool,
    mp_to_async_queue,
    multiplex_and_merge_async_queues,
    multiplex_async_queues,
)


class TestSentinel:
    """Test SENTINEL object."""
    
    def test_sentinel_is_object(self) -> None:
        assert SENTINEL is not None
        assert isinstance(SENTINEL, object)
        
    def test_sentinel_identity(self) -> None:
        # SENTINEL should maintain identity
        assert SENTINEL is SENTINEL
        
    def test_sentinel_comparison(self) -> None:
        assert SENTINEL == SENTINEL
        assert not (SENTINEL != SENTINEL)


class TestMPToAsyncBridge:
    """Test MP to async queue bridging."""
    
    @pytest.mark.asyncio
    async def test_mp_to_async_basic(self) -> None:
        """Test basic MP to async queue forwarding."""
        ctx = get_context("spawn")
        mp_queue = ctx.Queue(maxsize=5)
        
        # Add test data
        mp_queue.put(1)
        mp_queue.put(2) 
        mp_queue.put(3)
        mp_queue.put(SENTINEL)
        
        # Convert to async queue
        async_queue = mp_to_async_queue(mp_queue)
        
        # Collect results
        results = []
        while True:
            item = await async_queue.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        assert results == [1, 2, 3]
    
    @pytest.mark.asyncio
    async def test_mp_to_async_empty_queue(self) -> None:
        """Test MP to async with empty queue."""
        ctx = get_context("spawn")
        mp_queue = ctx.Queue(maxsize=1)
        mp_queue.put(SENTINEL)  # Only sentinel
        
        async_queue = mp_to_async_queue(mp_queue)
        
        first_item = await async_queue.get()
        assert first_item is SENTINEL
    
    @pytest.mark.asyncio  
    async def test_mp_to_async_large_data(self) -> None:
        """Test MP to async with larger dataset."""
        ctx = get_context("spawn")
        mp_queue: MPQueue = ctx.Queue(maxsize=50)
        
        # Add lots of data
        test_data = list(range(20))
        for item in test_data:
            mp_queue.put(item)
        mp_queue.put(SENTINEL)
        
        async_queue: Queue = mp_to_async_queue(mp_queue)
        
        results = []
        while True:
            got = await async_queue.get()
            if got is SENTINEL:
                break
            results.append(got)
        
        assert results == test_data


class TestAsyncToMPBridge:
    """Test async to MP queue bridging."""
    
    @pytest.mark.asyncio
    async def test_async_to_mp_basic(self) -> None:
        """Test basic async to MP queue forwarding."""
        async_queue: Queue = asyncio.Queue(maxsize=5)
        
        # Add test data
        await async_queue.put("hello")
        await async_queue.put("world")
        await async_queue.put(SENTINEL)
        
        # Convert to MP queue (race-condition-free version)
        mp_queue = await async_to_mp_queue_with_ready(async_queue)
        
        # Collect results from MP queue
        results = []
        while True:
            item = mp_queue.get(timeout=1)
            if item == SENTINEL:
                break
            results.append(item)
        
        assert results == ["hello", "world"]
    
    @pytest.mark.asyncio
    async def test_async_to_mp_with_numbers(self) -> None:
        """Test async to MP with numeric data."""
        async_queue: Queue[Any] = asyncio.Queue(maxsize=10)
        
        test_numbers = [1, 2, 3, 4, 5]
        for num in test_numbers:
            await async_queue.put(num)
        await async_queue.put(SENTINEL)
        
        mp_queue = await async_to_mp_queue_with_ready(async_queue, ctx_method="spawn")
        
        results = []
        while True:
            item = mp_queue.get(timeout=1)
            if item is SENTINEL:
                break
            results.append(item)
        
        assert results == test_numbers


class TestAsyncMultiplexing:
    """Test async queue multiplexing."""
    
    @pytest.mark.asyncio
    async def test_multiplex_single_queue(self) -> None:
        """Test multiplexing with single queue."""
        queue1: Queue[Any] = asyncio.Queue(maxsize=5)
        
        await queue1.put("item1")
        await queue1.put("item2")
        await queue1.put(SENTINEL)
        
        multiplexed = multiplex_async_queues([queue1])
        
        results = []
        while True:
            item = await multiplexed.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        assert results == ["item1", "item2"]
    
    @pytest.mark.asyncio
    async def test_multiplex_multiple_queues(self) -> None:
        """Test multiplexing multiple queues."""
        queue1: Queue[Any] = asyncio.Queue(maxsize=5)
        queue2: Queue[Any] = asyncio.Queue(maxsize=5)
        
        # Add data to both queues
        await queue1.put("q1_item1")
        await queue1.put("q1_item2")
        await queue1.put(SENTINEL)
        
        await queue2.put("q2_item1")
        await queue2.put("q2_item2")  
        await queue2.put(SENTINEL)
        
        multiplexed = multiplex_async_queues([queue1, queue2])
        
        results = []
        while True:
            item = await multiplexed.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        # Should get all items from both queues
        assert len(results) == 4
        assert "q1_item1" in results
        assert "q1_item2" in results
        assert "q2_item1" in results
        assert "q2_item2" in results
    
    # @pytest.mark.asyncio
    # async def test_multiplex_empty_queue_list(self):
    #     """Test multiplexing with empty queue list."""
    #     multiplexed = multiplex_async_queues([])
        
    #     # Should get SENTINEL immediately
    #     item = await multiplexed.get()
    #     assert item is SENTINEL
    
    @pytest.mark.asyncio
    async def test_multiplex_and_merge(self) -> None:
        """Test multiplexing with merge function."""
        queue1: Queue[Any] = asyncio.Queue(maxsize=5)
        queue2: Queue[Any] = asyncio.Queue(maxsize=5)
        
        # Add synchronized data
        await queue1.put(1)
        await queue2.put(2)
        await queue1.put(10)
        await queue2.put(20)
        await queue1.put(SENTINEL)
        await queue2.put(SENTINEL)
        
        def sum_merge(items: List) -> int:
            return sum(items)
        
        multiplexed = multiplex_and_merge_async_queues([queue1, queue2], sum_merge)
        
        results = []
        while True:
            item = await multiplexed.get()
            if item is SENTINEL:
                break
            results.append(item)
        
        # Should get merged results: 1+2=3, 10+20=30
        assert results == [3, 30]


class TestSharedInbound:
    """Test shared inbound queue creation."""
    
    @pytest.mark.asyncio
    async def test_make_shared_inbound_basic(self) -> None:
        """Test creating shared inbound queue."""
        upstream: Queue[Any] = asyncio.Queue(maxsize=10)
        
        # Add test data
        await upstream.put(1)
        await upstream.put(2)
        await upstream.put(3)
        await upstream.put(SENTINEL)
        
        shared = await make_shared_inbound_for_pool(upstream, n_workers=2, maxsize=5)
        
        # Should be able to get items from shared queue
        items = []
        sentinel_count = 0
        
        # Collect items until we get 2 SENTINELs (one per worker)
        while sentinel_count < 2:
            item = await shared.get()
            if item is SENTINEL:
                sentinel_count += 1
            else:
                items.append(item)
        
        assert set(items) == {1, 2, 3}
        assert sentinel_count == 2
    
    @pytest.mark.asyncio
    async def test_shared_inbound_single_worker(self) -> None:
        """Test shared inbound with single worker."""
        upstream: Queue[Any] = asyncio.Queue(maxsize=5)
        
        await upstream.put("test")
        await upstream.put(SENTINEL)
        
        shared = await make_shared_inbound_for_pool(upstream, n_workers=1)
        
        item1 = await shared.get()
        assert item1 == "test"
        
        item2 = await shared.get()
        assert item2 is SENTINEL
    
    @pytest.mark.asyncio
    async def test_shared_inbound_empty_upstream(self) -> None:
        """Test shared inbound with empty upstream."""
        upstream: Queue[Any] = asyncio.Queue(maxsize=1)
        await upstream.put(SENTINEL)
        
        shared = await make_shared_inbound_for_pool(upstream, n_workers=3)
        
        # Should get 3 SENTINELs
        sentinels = []
        for _ in range(3):
            item = await shared.get()
            sentinels.append(item)
        
        assert all(item is SENTINEL for item in sentinels)


class TestBroadcastInbounds:
    """Test broadcast inbound queues."""
    
    @pytest.mark.asyncio
    async def test_make_broadcast_basic(self) -> None:
        """Test creating broadcast inbound queues."""
        upstream: Queue[Any] = asyncio.Queue(maxsize=10)
        
        await upstream.put("broadcast_item")
        await upstream.put(SENTINEL)
        
        broadcast_queues = await make_broadcast_inbounds(upstream, sizes=[3, 3])
        
        assert len(broadcast_queues) == 2
        
        # Both queues should receive the same item
        item1 = await broadcast_queues[0].get()
        item2 = await broadcast_queues[1].get()
        
        assert item1 == "broadcast_item"
        assert item2 == "broadcast_item"
        
        # Both should get SENTINEL
        sentinel1 = await broadcast_queues[0].get()
        sentinel2 = await broadcast_queues[1].get()
        
        assert sentinel1 is SENTINEL
        assert sentinel2 is SENTINEL
    
    @pytest.mark.asyncio
    async def test_broadcast_multiple_items(self) -> None:
        """Test broadcast with multiple items."""
        upstream: Queue[Any] = asyncio.Queue(maxsize=10)
        
        test_items = ["a", "b", "c"]
        for item in test_items:
            await upstream.put(item)
        await upstream.put(SENTINEL)
        
        broadcast_queues = await make_broadcast_inbounds(upstream, sizes=[5, 5, 5])
        
        # Each queue should receive all items
        for queue in broadcast_queues:
            received_items = []
            while True:
                got = await queue.get()
                if got is SENTINEL:
                    break
                received_items.append(got)
            
            assert received_items == test_items
    
    @pytest.mark.asyncio
    async def test_broadcast_different_buffer_sizes(self) -> None:
        """Test broadcast with different buffer sizes."""
        upstream: Queue[Any] = asyncio.Queue(maxsize=5)
        
        await upstream.put("item")
        await upstream.put(SENTINEL)
        
        # Different buffer sizes
        broadcast_queues = await make_broadcast_inbounds(upstream, sizes=[1, 10, 5])
        
        assert len(broadcast_queues) == 3
        assert broadcast_queues[0].maxsize == 1
        assert broadcast_queues[1].maxsize == 10
        assert broadcast_queues[2].maxsize == 5
        
        # All should receive the item
        for queue in broadcast_queues:
            item = await queue.get()
            assert item == "item"
            
            sentinel = await queue.get()
            assert sentinel is SENTINEL
    
    @pytest.mark.asyncio
    async def test_broadcast_empty_sizes(self) -> None:
        """Test broadcast with empty sizes list."""
        upstream: Queue[Any] = asyncio.Queue(maxsize=5)
        await upstream.put(SENTINEL)
        
        broadcast_queues = await make_broadcast_inbounds(upstream, sizes=[])
        
        assert len(broadcast_queues) == 0


class TestAsyncUtilIntegration:
    """Integration tests combining multiple async_util functions."""
    
    @pytest.mark.asyncio
    async def test_mp_async_mp_roundtrip(self) -> None:
        """Test MP->Async->MP roundtrip."""
        # Start with MP queue
        ctx = get_context("spawn")
        mp_queue1 = ctx.Queue(maxsize=5)
        
        mp_queue1.put("test_item")
        mp_queue1.put(SENTINEL)
        
        # Convert to async
        async_queue = mp_to_async_queue(mp_queue1)
        
        # Convert back to MP
        mp_queue2 = async_to_mp_queue(async_queue)
        
        # Allow conversion time
        await asyncio.sleep(0.01)
        
        # Should get same data
        item = mp_queue2.get(timeout=1)
        assert item == "test_item"
        
        sentinel = mp_queue2.get(timeout=1)
        assert sentinel is SENTINEL
    
    @pytest.mark.asyncio
    async def test_complex_multiplexing_workflow(self) -> None:
        """Test complex workflow with multiple multiplexing operations."""
        # Create multiple source queues
        sources = []
        for i in range(3):
            queue: Queue[Any] = asyncio.Queue(maxsize=5)
            await queue.put(f"item_{i}_1")
            await queue.put(f"item_{i}_2")
            await queue.put(SENTINEL)
            sources.append(queue)
        
        # Multiplex them
        multiplexed = multiplex_async_queues(sources)
        
        # Create shared inbound from multiplexed result
        shared = await make_shared_inbound_for_pool(multiplexed, n_workers=2)
        
        # Collect all items
        items = []
        sentinel_count = 0
        
        while sentinel_count < 2:  # Wait for both worker SENTINELs
            item = await shared.get()
            if item is SENTINEL:
                sentinel_count += 1
            else:
                items.append(item)
        
        # Should have all items from all sources
        assert len(items) == 6  # 3 sources * 2 items each
        
        # All expected items should be present
        for i in range(3):
            assert f"item_{i}_1" in items
            assert f"item_{i}_2" in items
