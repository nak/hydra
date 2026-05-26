#  Copyright (c) 2025.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import logging
import os
import ssl
from contextlib import contextmanager
from typing import TypeVar, Generic, Generator

import hydra.ssl_contexts
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

    def __init__(self, name: str, address: tuple[str, int], sentinel: S, ssl_context: ssl.SSLContext | None = None):
        super().__init__()
        self._closed = False
        self._name = name
        self._address = address
        self._ssl_context = ssl_context
        self._joinable_queue = SinkJoinableQueue[T, S](address=address, sentinel=sentinel, size=0)

    def __enter__(self):
        self._joinable_queue.register(self._name, self._ssl_context)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __getstate__(self) -> dict[str, object]:
        """
        Return only what a client queue needs to connect to the server, not the internal state of the queue/server.
        """
        return {'_name': self._name, '_address': self._address, '_closed': self._closed,
                '_ssl_context': hydra.ssl_contexts.extract_ssl_context_info(self._ssl_context)
                if self._ssl_context else None}

    def __setstate__(self, state):
        state['_ssl_context'] = hydra.ssl_contexts.rebuild_ssl_context(state['_ssl_context'])\
            if state.get('_ssl_context') else None
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
        if self._joinable_queue.transact(
            self._address, self._joinable_queue.ACTION_PUT, (item, timeout),
            timeout_io=self._joinable_queue.TIMEOUT_SOCKET_IO, ssl_context=self._ssl_context
        ) != 0:
            raise RuntimeError("Failed to put data to joinable queue server")

    def close(self):
        """
        Close the connection to the remote queue.
        """
        if not self._closed and self._joinable_queue.unregister(self._name, ssl_context=self._ssl_context) != 0:
            logger.error("Failed to send closure status to remote queue")
        self._closed = True


class SinkQueueConsumer(Generic[T, S], SinkConsumer[T]):
    """
    Joinable queue that can be used as a sink for items to be processed from multiple remote clients
    """

    def __init__(self, name: str, address: tuple[str, int], sentinel: S,
                 ssl_context: ssl.SSLContext | None = None, size: int = 0):
        super().__init__()
        self._name = name
        self._address = address
        self._client_ssl_context = ssl_context
        self._joinable_queue = SinkJoinableQueue[T, S](address=address, sentinel=sentinel, size=size)

    @contextmanager
    def start(self, server_ssl_context: ssl.SSLContext) -> Generator["SinkQueueConsumer[T, S]", None, None]:
        self._joinable_queue.start(server_ssl_context)
        try:
            yield self
        finally:
            self._joinable_queue.shutdown()

    def __getstate__(self) -> dict[str, object]:
        """
        Return only what a client queue needs to connect to the server, not the internal state of the queue/server.
        """
        return {
            '_name': self._name, '_address': self._address,
            '_client_ssl_context': hydra.ssl_contexts.extract_ssl_context_info(self._client_ssl_context)
            if self._client_ssl_context else None,
        }

    def __setstate__(self, state):
        state['_client_ssl_context'] = hydra.ssl_contexts.rebuild_ssl_context(state['_client_ssl_context'])\
            if state.get('_client_ssl_context') else None
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
        return self._joinable_queue.transact(self._address, self._joinable_queue.ACTION_GET, payload=timeout,
                                             ssl_context=self._client_ssl_context)

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
