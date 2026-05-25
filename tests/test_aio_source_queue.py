#  Copyright (c) 2026.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import asyncio

import pytest


@pytest.mark.asyncio
async def test_server_and_client_async_queue(free_port: int):
    from hydra.distributed_queues.aio_source_queue import AsyncSourceQueueFeed, AsyncSourceQueueConsumer

    localhost = '127.0.0.1'

    async with AsyncSourceQueueFeed[int](name="pytest_server", address=(localhost, free_port), size=10) as server_queue:
      async with AsyncSourceQueueConsumer[int](name="pytest_client", address=(localhost, free_port)) as client_queue:

        for num in range(20):
            if num >= 10:
                assert await client_queue.get() == num - 10
                await client_queue.task_started(f'task{num}')
                await client_queue.task_done(f'task{num}')
            await server_queue.put(num)

        for num in range(10, 20):
            with pytest.raises(asyncio.TimeoutError):
                await server_queue.join(timeout=0.1)
            assert await client_queue.get() == num
            await client_queue.task_started(f'task{num}')
            await client_queue.task_done(f'task{num}')
            with pytest.raises(RuntimeError):
                await client_queue.task_done(f'task{num + 1}')

        with pytest.raises(asyncio.queues.QueueEmpty):
            item = await client_queue.get(timeout=0.1)
            assert False, f"Expected QueueEmpty exception, but got item: {item}"


    await server_queue.join(timeout=0.1)
