#  Copyright (c) 2025.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import asyncio
import os
import ssl
from contextlib import asynccontextmanager, suppress
from pickle import PickleError
from typing import TypeVar, AsyncGenerator

import hydra.ssl_contexts
from hydra.distributed_queues.aio_queue_api import AsyncConsumer, AsyncSourceFeed
from hydra.distributed_queues.configuration import SSLCertificatesConfig
from hydra.distributed_queues.joinable_queue import SourceJoinableQueue, SinkJoinableQueue

T = TypeVar('T')


class AsyncSourceQueueConsumer(AsyncConsumer[T]):
    """
    A consumer of items from a remote queue.
    """
    _pickle_counter = 0

    def __init__(self, name: str, address: tuple[str, int], ssl_context: ssl.SSLContext | None = None):
        super().__init__()
        self._name = name
        self._address = address
        self._ssl_context = ssl_context
        self._joinable_queue = SourceJoinableQueue[T, None](address)
        self._closed = True
        self._pickle_task: asyncio.Task | None = None

    @property
    def address(self):
        return self._address

    @property
    def name(self):
        return self._name

    async def __aenter__(self):
        if self._closed:
            await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def connect(self):
        """
        This connects to the server to register the feed.  The queue is not usable until this call is made,
        which can also be done by using the queue as a context manager.  Note that the connection is closed
        once the register with the server is complete.  All calls to eh API requiring a server transaction, connect
        to ths server, perform the requested action, and the disconnects.
        """
        await self._joinable_queue.register_async(self._name, self._ssl_context)
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
            try:
                asyncio.get_running_loop()
                # register if not closed to ensure unique registration and reflect proper state
                self._pickle_task = \
                    asyncio.create_task(self._joinable_queue.register_async(self._name, self._ssl_context))
            except RuntimeError:
                asyncio.run(self._joinable_queue.register_async(self._name, self._ssl_context))

    def __del__(self):
        if not self._closed:
            asyncio.run(self.close())

    async def get(self, timeout: float | None = None) -> T | None:
        """
        Get an item from the remote server source-queue.

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
        return await self._joinable_queue.transact_async(
            self._address, self._joinable_queue.ACTION_GET, payload=timeout, ssl_context=self._ssl_context,
            timeout=timeout
        )

    async def task_started(self, task: T | None = None) -> None:
        """
        Notify the remote server source-queue that the task is started.

        Args:
            task: optional task that was started.  If None, the server will not track specific tasks, only
            the count of tasks in progress.  Otherwise, the server tracks tasks individually
        """
        if await self._joinable_queue.transact_async(
                self._address, self._joinable_queue.ACTION_TASK_STARTED, payload=task, ssl_context=self._ssl_context
        ) != 0:
            raise RuntimeError("Failed to notify remote queue that task is done")

    async def task_done(self, task: T | None = None) -> None:
        """
        Notify the remote queue that the task is done.

        Args:
            task: optional task that was started.  If None, the server will not track specific tasks, only
            the count of tasks in progress.  Otherwise, the server tracks tasks individually
        """
        if self._closed:
            raise RuntimeError("Queue is closed and cannot notify task done")
        if await self._joinable_queue.transact_async(
            self._address, self._joinable_queue.ACTION_TASK_DONE, payload=task, ssl_context=self._ssl_context
        ) != 0:
            raise RuntimeError("Failed to notify remote queue that task is done")

    async def close(self):
        """
        Close the connection to the remote queue.  After this call, not more operations can be performed on the queue
        until another connect call is mase.
        """
        if not self._closed:
            with suppress(ConnectionError):
                await self._joinable_queue.unregister_async(self._name, self._ssl_context)
        self._closed = True


class AsyncSourceQueueFeed(AsyncSourceFeed[T]):
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

    @asynccontextmanager
    async def start(self, server_ssl_context: ssl.SSLContext | None = None)\
            -> AsyncGenerator["AsyncSourceQueueFeed[T]", None]:
        """
        Start a background task to serve the source-queue.
        """
        await self._joinable_queue.start_async(server_ssl_context)
        try:
            yield self
        finally:
            self._joinable_queue.shutdown()

    def __getstate__(self) -> dict[str, object]:
        """
        Return only what a client queue needs to connect to the server, not the internal state of the queue/server.
        """
        raise PickleError("AsyncSourceQueueFeed cannot be pickled as it is not to be duplicated")

    def __setstate__(self, state):
        raise PickleError("AsyncSourceQueueFeed cannot be (un)pickled as it is not to be duplicated")

    async def put(self, item: T, timeout: float | None = None) -> None:
        """
        Post data to the remote server source-queue.

        Args:
            item: item to put in the queue.
            timeout: The timeout for the transaction.
        """
        if await self._joinable_queue.transact_async(
            self._address, self._joinable_queue.ACTION_PUT, (item, timeout), timeout=timeout,
            ssl_context=self._client_ssl_context
        ) != 0:
            raise RuntimeError("Failed to put data to joinable queue server")

    async def join(self, timeout: float | None = None) -> None:
        """
        Wait for all items in the queue to be processed, as determined by the number of tasks done vs the number of
        items put in the queue.

        Args:
            timeout: The timeout for the transaction.

        Raises:
            RuntimeError: if the server returns an error
        """
        if await self._joinable_queue.transact_async(
            self._address, self._joinable_queue.ACTION_JOIN, payload=timeout, ssl_context=self._client_ssl_context,
            timeout=timeout
        ) != 0:
            raise RuntimeError("Failed to join joinable queue server")
