#  Copyright (c) 2025.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
from typing import TypeVar, Generic

from hydra.distributed_queues.aio_queue_api import AsyncFeed, AsyncConsumer
from hydra.distributed_queues.joinable_queue import SinkJoinableQueue

T = TypeVar('T')
S = TypeVar('S')


class AsyncSinkQueueFeed(AsyncFeed[T]):
    """
    Joinable queue that can be used as a sink to put items to be processed by a remote queue/server.
    """

    def __init__(self, name: str, address: tuple[str, int], sentinel: S):
        super().__init__()
        self._name = name
        self._address = address
        self._closed = True
        self._joinable_queue = SinkJoinableQueue[T, S](address=address, sentinel=sentinel)

    async def __aenter__(self):
        await self._joinable_queue.register_async(self._name)
        self._closed = False
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __getstate__(self) -> dict[str, object]:
        """
        Return only what a client queue needs to connect to the server, not the internal state of the queue/server.
        """
        return {'name': self._name, 'address': self._address, '_closed': self._closed}

    def __setstate__(self, state):
        self.__dict__.update(state)

    async def put(self, item: T, timeout: float | None = None) -> None:
        """
        Post data to the joinable queue server.

        Args:
            item: item to put in the queue.
            timeout: The timeout for the transaction.

        """
        if self._closed:
            raise RuntimeError("Queue is closed and cannot put items")
        if await self._joinable_queue.transact_async(
                self._address, self._joinable_queue.ACTION_PUT, (item, timeout),
                timeout_io=self._joinable_queue.TIMEOUT_SOCKET_IO) != 0:
            raise RuntimeError("Failed to put data to joinable queue server")

    async def close(self):
        """
        Close the connection to the remote queue.
        """
        if not self._closed:
            await self._joinable_queue.unregister_async(self._name)


class AsyncSinkQueueConsumer( Generic[T, S], AsyncConsumer[T]):
    """
    Joinable queue that can be used as a sink for items to be processed from multiple remote clients
    """

    def __init__(self, name: str, address: tuple[str, int], sentinel: S, size: int = 0):
        super().__init__()
        self._name = name
        self._address = address
        self._joinable_queue = SinkJoinableQueue[T, S](address=address, sentinel=sentinel, size=size)

    async def __aenter__(self):
        await self._joinable_queue.start_async()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._joinable_queue.shutdown()

    def __getstate__(self) -> dict[str, object]:
        """
        Return only what a client queue needs to connect to the server, not the internal state of the queue/server.
        """
        return {'name': self._name, 'address': self._address}

    def __setstate__(self, state):
        self.__dict__.update(state)

    async def get(self, timeout: float | None = None) -> T | S:
        """
        Get an item from the joinable queue server.

        Args:
            timeout: The timeout for the transaction.

        Returns:
            The item retrieved from the queue.

        Raises:
            QueueEmpty: if not item is available in queue in time
            RuntimeError: if the server returns an error
        """
        return await self._joinable_queue.transact_async(
            self._address, self._joinable_queue.ACTION_GET, payload=timeout
        )


    async def join(self, timeout: float | None = None) -> None:
        """
        Wait for all items in the queue to be processed.

        Args:
            timeout: The timeout for the transaction.

        Raises:
            RuntimeError: if the server returns an error
        """
        if await self._joinable_queue.transact_async(
                self._address, self._joinable_queue.ACTION_JOIN, payload=timeout) != 0:
            raise RuntimeError("Failed to join joinable queue server")
