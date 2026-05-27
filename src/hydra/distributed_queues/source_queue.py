#  Copyright (c) 2025.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import os
import ssl
from contextlib import contextmanager, suppress
from pickle import PickleError
from typing import TypeVar

import hydra.ssl_contexts
from hydra.distributed_queues.configuration import SSLCertificatesConfig
from hydra.distributed_queues.joinable_queue import SourceJoinableQueue, SinkJoinableQueue
from hydra.distributed_queues.queue_api import Consumer, SourceFeed

T = TypeVar('T')


class SourceQueueConsumer(Consumer[T]):
    """
    A consumer of items from a remote queue.
    """
    _pickle_counter = 0

    def __init__(self, name: str, address: tuple[str, int], ssl_context: ssl.SSLContext | None = None):
        super().__init__()
        self._closed = True
        self._name = name
        self._address = address
        self._ssl_context = ssl_context
        self._joinable_queue = SourceJoinableQueue[T, None](address=address, size=0)

    @property
    def address(self):
        return self._address

    @property
    def name(self):
        return self._name

    def __enter__(self):
        if self._closed:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """
        This connects to the server to register the feed.  The queue is not usable until this call is made,
        which can also be done by using the queue as a context manager.  Note that the connection is closed
        once the register with the server is complete.  All calls to eh API requiring a server transaction, connect
        to the server, perform the requested action, and the disconnects.
        """
        self._joinable_queue.register(self._name, self._ssl_context)
        self._closed = False

    def __getstate__(self) -> dict[str, object]:
        """
        Return only what a client queue needs to connect to the server, not the internal state of the queue/server.
        """
        return {'_name': self._name, '_address': self._address, '_closed': self._closed,
                '_ssl_context': hydra.ssl_contexts.extract_ssl_context_info(self._ssl_context)
                if self._ssl_context else None}

    def __setstate__(self, state):
        if state.get('_ssl_context'):
            new_context = ssl.SSLContext(state['_ssl_context']['protocol'])
            SSLCertificatesConfig.reload_certificates(new_context, state['_ssl_context'])
        else:
            new_context = None
        self.__class__._pickle_counter += 1
        state['_ssl_context'] = new_context
        state['_name'] = f"{state['_name']}-{state['_address'][0]}-{os.getpid()}-{self._pickle_counter}"
        state['_joinable_queue'] = SinkJoinableQueue(address=state['_address'], sentinel=None)
        self.__dict__.update(state)
        if not self._closed:
            self._joinable_queue.register(self._name, self._ssl_context)

    def __del__(self):
        if not self._closed:
            self.close()

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
        return self._joinable_queue.transact(self._address, self._joinable_queue.ACTION_GET, payload=timeout,
                                             ssl_context=self._ssl_context)

    def task_started(self, task: T | None = None) -> None:
        """
        Notify the remote queue that the task is done.
        """
        if self._closed:
            raise RuntimeError("Queue is closed and cannot notify task started")
        if self._joinable_queue.transact(
            self._address, self._joinable_queue.ACTION_TASK_STARTED, payload=task, ssl_context=self._ssl_context
        ) != 0:
            raise RuntimeError("Failed to notify remote queue that task is started")

    def task_done(self, task: T | None = None) -> None:
        """
        Notify the remote queue that the task is done.
        """
        if self._closed:
            raise RuntimeError("Queue is closed and cannot notify task done")
        if self._joinable_queue.transact(
            self._address, self._joinable_queue.ACTION_TASK_DONE, payload=task, ssl_context=self._ssl_context
        ) != 0:
            raise RuntimeError("Failed to notify remote queue that task is done")

    def close(self):
        """
        Close the connection to the remote queue.
        """
        with suppress(TimeoutError, ConnectionError):
            if not self._closed:
                self._joinable_queue.unregister_async(self._name, self._ssl_context)
        self._closed = True


class SourceQueueFeed(SourceFeed[T]):
    """
    A joinable queue that can be used as a source to put items to be processed by multiple remote clients
    """

    def __init__(self, address: tuple[str, int], size: int, ssl_context: ssl.SSLContext | None = None):
        super().__init__()
        self._address = address
        self._client_ssl_context = ssl_context
        self._joinable_queue = SourceJoinableQueue[T, None](address=address, size=size)

    @property
    def address(self):
        return self._address

    @contextmanager
    def start(self, server_ssl_context: ssl.SSLContext | None = None):
        """
        Context manager to start the joinable queue server and ensure it is properly shut down.

        Args:
            server_ssl_context: Optional SSL context for the server.
        """
        self._joinable_queue.start(server_ssl_context)
        try:
            yield self
        finally:
            self._joinable_queue.shutdown()

    def __getstate__(self):
        raise PickleError("SinkQueueConsumer is not intended to be pickled as it is a server-side consumer")

    def __setstate__(self, state):
        raise PickleError("SinkQueueConsumer is not intended to be pickled as it is a server-side consumer")

    def put(self, item: T, timeout: float | None = None) -> None:
        """
        Post data to the joinable queue server.

        Args:
            item: item to put in the queue.
            timeout: The timeout for the transaction.
        """
        if self._joinable_queue.transact(
            self._address, self._joinable_queue.ACTION_PUT, (item, timeout),
            timeout=self._joinable_queue.TIMEOUT_SOCKET_IO, ssl_context=self._client_ssl_context
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
        if self._joinable_queue.transact(self._address, self._joinable_queue.ACTION_JOIN, payload=timeout,
                                         ssl_context=self._client_ssl_context) != 0:
            raise RuntimeError("Failed to join joinable queue server")

    def close(self) -> None:
        pass
