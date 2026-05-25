#  Copyright (c) 2025.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
from typing import TypeVar

from hydra.distributed_queues.joinable_queue import SourceJoinableQueue
from hydra.distributed_queues.queue_api import Consumer, SourceFeed

T = TypeVar('T')


class SourceQueueConsumer(Consumer[T]):
    """
    A consumer of items from a remote queue.
    """

    def __init__(self, name: str, address: tuple[str, int]):
        super().__init__()
        self._closed = True
        self._name = name
        self._address = address
        self._joinable_queue = SourceJoinableQueue[T, None](address=address, size=0)

    def __enter__(self):
        self._closed = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self._closed = True

    def __getstate__(self) -> dict[str, object]:
        """
        Return only what a client queue needs to connect to the server, not the internal state of the queue/server.
        """
        return {'name': self._name, 'address': self._address, '_closed': self._closed}

    def __setstate__(self, state):
        self.__dict__.update(state)

    def get(self, timeout: float | None = None) -> T | None:
        """
        Get an item from the joinable queue server.

        Args:
            timeout: The timeout for the transaction.

        Returns:
            The item retrieved from the queue.

        Raises:
            QueueEmpty: if no item is available in the queue in time
            RuntimeError: if the server returns an error
        """
        if self._closed:
            raise RuntimeError("Queue is closed and cannot get items")
        return self._joinable_queue.transact(self._address, self._joinable_queue.ACTION_GET, payload=timeout)

    def task_started(self, task: T | None = None) -> None:
        """
        Notify the remote queue that the task is done.
        """
        if self._closed:
            raise RuntimeError("Queue is closed and cannot notify task started")
        if self._joinable_queue.transact(self._address, self._joinable_queue.ACTION_TASK_STARTED, payload=task) != 0:
            raise RuntimeError("Failed to notify remote queue that task is started")

    def task_done(self, task: T | None = None) -> None:
        """
        Notify the remote queue that the task is done.
        """
        if self._closed:
            raise RuntimeError("Queue is closed and cannot notify task done")
        if self._joinable_queue.transact(self._address, self._joinable_queue.ACTION_TASK_DONE, payload=task) != 0:
            raise RuntimeError("Failed to notify remote queue that task is done")

    def close(self):
        """
        Close the connection to the remote queue.
        """
        self._closed = True


class SourceQueueFeed(SourceFeed[T]):
    """
    A joinable queue that can be used as a source to put items to be processed by multiple remote clients
    """

    def __init__(self, name: str, address: tuple[str, int], size: int):
        super().__init__()
        self._name = name
        self._address = address
        self._joinable_queue = SourceJoinableQueue[T, None](address=address, size=size)

    def __enter__(self):
        self._joinable_queue.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._joinable_queue.shutdown()

    def put(self, item: T, timeout: float | None = None) -> None:
        """
        Post data to the joinable queue server.

        Args:
            item: item to put in the queue.
            timeout: The timeout for the transaction.
        """
        if self._joinable_queue.transact(
            self._address, self._joinable_queue.ACTION_PUT, (item, timeout),
            timeout_io=self._joinable_queue.TIMEOUT_SOCKET_IO
        ) != 0:
            raise RuntimeError("Failed to put data to joinable queue server")

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

    def close(self) -> None:
        pass
