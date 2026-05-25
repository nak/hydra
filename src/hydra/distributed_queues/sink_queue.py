#  Copyright (c) 2025.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import logging
import os
from typing import TypeVar, Generic

from hydra.distributed_queues.joinable_queue import SinkJoinableQueue
from hydra.distributed_queues.queue_api import Feed, SinkConsumer

T = TypeVar('T')
S = TypeVar('S')

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("PYTEST_MPROC_LOG_LEVEL", "WARNING").upper())
logger.addHandler(logging.StreamHandler())


class SinkQueueFeed(Feed[T]):
    """
    Joinable queue that can be used as a sink to put items to be processed by a remote queue/server.
    """

    def __init__(self, name: str, address: tuple[str, int], sentinel: S):
        super().__init__()
        self._closed = False
        self._name = name
        self._address = address
        self._joinable_queue = SinkJoinableQueue[T, S](address=address, sentinel=sentinel, size=0)

    def __enter__(self):
        self._joinable_queue.register(self._name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __getstate__(self) -> dict[str, object]:
        """
        Return only what a client queue needs to connect to the server, not the internal state of the queue/server.
        """
        return {'name': self._name, 'address': self._address, '_closed': self._closed}

    def __setstate__(self, state):
        self.__dict__.update(state)

    def put(self, item: T, timeout: float | None = None) -> None:
        """
        Post data to the joinable queue server.

        Args:
            item: item to put in the queue.
            timeout: The timeout for the transaction.

        """
        if self._closed:
            raise RuntimeError("Queue is closed and cannot put items")
        if self._joinable_queue.transact(self._address, self._joinable_queue.ACTION_PUT, (item, timeout),
                                         timeout_io=self._joinable_queue.TIMEOUT_SOCKET_IO) != 0:
            raise RuntimeError("Failed to put data to joinable queue server")

    def close(self):
        """
        Close the connection to the remote queue.
        """
        if not self._closed and self._joinable_queue.unregister(self._name) != 0:
            logger.error("Failed to send closure status to remote queue")
        self._closed = True


class SinkQueueConsumer(Generic[T, S], SinkConsumer[T]):
    """
    Joinable queue that can be used as a sink for items to be processed from multiple remote clients
    """

    def __init__(self, name: str, address: tuple[str, int], sentinel: S, size: int = 0):
        super().__init__()
        self._name = name
        self._address = address
        self._joinable_queue = SinkJoinableQueue[T, S](address=address, sentinel=sentinel, size=size)

    def __enter__(self):
        self._joinable_queue.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._joinable_queue.shutdown()

    def __getstate__(self) -> dict[str, object]:
        """
        Return only what a client queue needs to connect to the server, not the internal state of the queue/server.
        """
        return {'name': self._name, 'address': self._address}

    def __setstate__(self, state):
        self.__dict__.update(state)

    def get(self, timeout: float | None = None) -> T | S:
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
        return self._joinable_queue.transact(self._address, self._joinable_queue.ACTION_GET, payload=timeout)

    def join(self, timeout: float | None = None) -> None:
        """
        Wait for all items in the queue to be processed.

        Args:
            timeout: The timeout for the transaction.

        Raises:
            RuntimeError: if the server returns an error
        """
        if self._joinable_queue.transact(self._address, self._joinable_queue.ACTION_JOIN, payload=timeout) != 0:
            raise RuntimeError("Failed to join joinable queue server")
