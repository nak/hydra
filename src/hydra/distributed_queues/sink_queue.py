#  Copyright (c) 2025.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import logging
import os
import ssl
from contextlib import contextmanager, suppress
from pickle import PickleError
from typing import TypeVar, Generic, Generator

import hydra.ssl_contexts
from hydra.distributed_queues.configuration import SSLCertificatesConfig
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
    _pickle_counter = 0

    def __init__(self, name: str, address: tuple[str, int], sentinel: S, ssl_context: ssl.SSLContext | None = None):
        super().__init__()
        self._closed = True
        self._name = name
        self._address = address
        self._ssl_context = ssl_context
        self._joinable_queue = SinkJoinableQueue[T, S](address=address, sentinel=sentinel, size=0)

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
        to ths server, perform the requested action, and the disconnects.
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
        state['_name'] = f"{state['_name']}-{state['_address'][0]}-{os.getpid()}-{self._pickle_counter}"
        state['_ssl_context'] = new_context
        state['_joinable_queue'] = SinkJoinableQueue(address=state['_address'], sentinel=None)
        self.__dict__.update(state)
        if not self._closed:
            self._joinable_queue.register(self._name, self._ssl_context)

    def __del__(self):
        if not self._closed:
            self.close()

    def put(self, item: T, timeout: float | None = None) -> None:
        """
        Post data to the remote server sink-queue.

        :param item: item to put in the queue.
        :param timeout: The timeout for the transaction.
        """
        if self._closed:
            raise RuntimeError("Queue is closed and cannot put items")
        if self._joinable_queue.transact(
            self._address, self._joinable_queue.ACTION_PUT, (item, timeout), timeout=timeout,
            ssl_context=self._ssl_context
        ) != 0:
            raise RuntimeError("Failed to put data to joinable queue server")

    def close(self):
        """
        Close this queue, unregistering it from the (remote) sink-queue.
        After this call, not more operations can be performed on the queue
        until another connect call is mase.
        """
        with suppress(TimeoutError, ConnectionError):
            if not self._closed:
                self._joinable_queue.unregister(self._name, ssl_context=self._ssl_context)
        self._closed = True


class SinkQueueConsumer(Generic[T, S], SinkConsumer[T]):
    """
    Joinable queue that can be used as a sink for items to be processed from multiple remote clients
    """

    def __init__(self, address: tuple[str, int], sentinel: S,
                 ssl_context: ssl.SSLContext | None = None, size: int = 0):
        super().__init__()
        self._address = address
        self._client_ssl_context = ssl_context
        self._joinable_queue = SinkJoinableQueue[T, S](address=address, sentinel=sentinel, size=size)

    @property
    def address(self):
        return self._address

    @contextmanager
    def start(self, server_ssl_context: ssl.SSLContext) -> Generator["SinkQueueConsumer[T, S]", None, None]:
        """
        Start a background task to serve the queue.

        :param server_ssl_context: optional SSL context for the server.
        """
        self._joinable_queue.start(server_ssl_context)
        try:
            yield self
        finally:
            self._joinable_queue.shutdown()

    def __getstate__(self) -> dict[str, object]:
        """
        Return only what a client queue needs to connect to the server, not the internal state of the queue/server.
        """
        raise PickleError("SinkQueueConsumer is not intended to be pickled as it is a server-side consumer")

    def __setstate__(self, state):
        raise PickleError("SinkQueueConsumer is not intended to be pickled as it is a server-side consumer")

    def get(self, timeout: float | None = None) -> T | S:
        """
        Get an item from the joinable queue server.

        :param timeout: The timeout for the transaction.

        :returns: The item retrieved from the queue.

        :raises QueueEmpty: if not item is available in queue in time
        :raises RuntimeError: if the server returns an error
        """
        return self._joinable_queue.transact(
            self._address, self._joinable_queue.ACTION_GET, payload=timeout, timeout=timeout,
            ssl_context=self._client_ssl_context
        )

    def join(self, timeout: float | None = None) -> None:
        """
        Wait for all clients to disconnect (unregister) before returning

        :param timeout: The timeout for the transaction.

        :raises RuntimeError: if the server returns an error
        """
        if self._joinable_queue.transact(self._address, self._joinable_queue.ACTION_JOIN, payload=timeout,
                                         ssl_context=self._client_ssl_context) != 0:
            raise RuntimeError("Failed to join joinable queue server")
