#  Copyright (c) 2026.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import asyncio

import pytest



def test_server_and_client_sync_queue(free_port: int):
    from hydra.distributed_queues.sink_queue import SinkQueueConsumer, SinkQueueFeed

    localhost = '127.0.0.1'

    with SinkQueueConsumer[int, str](name="pytest_server", address=(localhost, free_port), sentinel="done", size=10) as server_queue:
      with SinkQueueFeed[int](name="pytest_client", address=(localhost, free_port), sentinel="done") as client_queue:

        for num in range(20):
            if num >= 10:
                assert server_queue.get() == num - 10
            client_queue.put(num)

        for num in range(10, 20):
            assert server_queue.get() == num

        with pytest.raises(asyncio.queues.QueueEmpty):
            server_queue.get(timeout=0.1)

        with pytest.raises(asyncio.TimeoutError):
            server_queue.join(timeout=0.1)

    server_queue.join(timeout=0.1)
