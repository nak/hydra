#  Copyright (c) 2026.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import asyncio

import pytest


def test_server_and_client_sync_queue(free_port: int):
    from hydra.distributed_queues.source_queue import SourceQueueFeed, SourceQueueConsumer

    localhost = '127.0.0.1'

    with SourceQueueFeed[int](name="pytest_server", address=(localhost, free_port), size=10) as server_queue:
      with SourceQueueConsumer[int](name="pytest_client", address=(localhost, free_port)) as client_queue:

        for num in range(20):
            if num >= 10:
                assert client_queue.get() == num - 10
                client_queue.task_started(f'task{num}')
                client_queue.task_done(f'task{num}')
            server_queue.put(num)

        for num in range(10, 20):
            with pytest.raises(asyncio.TimeoutError):
                server_queue.join(timeout=0.1)
            assert client_queue.get() == num
            client_queue.task_started(f'task{num}')
            client_queue.task_done(f'task{num}')
            with pytest.raises(RuntimeError):
                client_queue.task_done(f'task{num + 1}')

        with pytest.raises(asyncio.queues.QueueEmpty):
            item = client_queue.get(timeout=0.1)
            assert False, f"Expected QueueEmpty exception, but got item: {item}"


    server_queue.join(timeout=0.1)
