#  Copyright (c) 2025.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import ssl
from contextlib import asynccontextmanager, suppress
from typing import TypeVar, AsyncGenerator

import hydra.ssl_contexts
from hydra.distributed_queues.aio_queue_api import AsyncConsumer, AsyncSourceFeed
from hydra.distributed_queues.joinable_queue import SourceJoinableQueue

T = TypeVar('T')


class AsyncSourceQueueConsumer(AsyncConsumer[T]):
    """
    A consumer of items from a remote queue.
    """

    def __init__(self, name: str, address: tuple[str, int], ssl_context: ssl.SSLContext | None = None):
        super().__init__()
        self._name = name
        self._address = address
        self._ssl_context = ssl_context
        self._joinable_queue = SourceJoinableQueue[T, None](address)
        self._closed = True

    @property
    def address(self):
        return self._address

    @property
    def name(self):
        return self._name

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def connect(self):
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
        state['_ssl_context'] = hydra.ssl_contexts.rebuild_ssl_context(state['_ssl_context'])\
            if state.get('_ssl_context') else None
        self.__dict__.update(state)

    async def get(self, timeout: float | None = None) -> T | None:
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
        return await self._joinable_queue.transact_async(
            self._address, self._joinable_queue.ACTION_GET, payload=timeout, ssl_context=self._ssl_context,
            timeout=timeout
        )

    async def task_started(self, task: T | None = None) -> None:
        """
        Notify the remote queue that the task is started.
        """
        if await self._joinable_queue.transact_async(
                self._address, self._joinable_queue.ACTION_TASK_STARTED, payload=task, ssl_context=self._ssl_context
        ) != 0:
            raise RuntimeError("Failed to notify remote queue that task is done")

    async def task_done(self, task: T | None = None) -> None:
        """
        Notify the remote queue that the task is done.
        """
        if self._closed:
            raise RuntimeError("Queue is closed and cannot notify task done")
        if await self._joinable_queue.transact_async(
            self._address, self._joinable_queue.ACTION_TASK_DONE, payload=task, ssl_context=self._ssl_context
        ) != 0:
            raise RuntimeError("Failed to notify remote queue that task is done")

    async def close(self):
        """
        Close the connection to the remote queue.
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
        await self._joinable_queue.start_async(server_ssl_context)
        try:
            yield self
        finally:
            self._joinable_queue.shutdown()

    def __getstate__(self) -> dict[str, object]:
        """
        Return only what a client queue needs to connect to the server, not the internal state of the queue/server.
        """
        return {
            '_address': self._address,
            '_client_ssl_context': hydra.ssl_contexts.extract_ssl_context_info(self._client_ssl_context)
            if self._client_ssl_context else None,
        }

    def __setstate__(self, state):
        state['_client_ssl_context'] = hydra.ssl_contexts.rebuild_ssl_context(state['_client_ssl_context'])\
            if state.get('_client_ssl_context') else None
        self.__dict__.update(state)

    async def put(self, item: T, timeout: float | None = None) -> None:
        """
        Post data to the joinable queue server.

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
        Wait for all items in the queue to be processed.

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
