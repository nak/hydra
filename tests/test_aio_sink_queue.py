#  Copyright (c) 2026.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import asyncio

import pytest



@pytest.mark.asyncio
async def test_server_and_client_async_queue(free_port: int):
    from hydra.distributed_queues.aio_sink_queue import AsyncSinkQueueConsumer, AsyncSinkQueueFeed

    localhost = '127.0.0.1'

    async with AsyncSinkQueueConsumer[int, str](name="pytest_server", address=(localhost, free_port), sentinel="done", size=10) as server_queue:
      async with AsyncSinkQueueFeed[int](name="pytest_client", address=(localhost, free_port), sentinel="done") as client_queue:

        for num in range(20):
            if num >= 10:
                assert await server_queue.get() == num - 10
            await client_queue.put(num)

        for num in range(10, 20):
            assert await server_queue.get() == num

        with pytest.raises(asyncio.queues.QueueEmpty):
            await server_queue.get(timeout=0.1)