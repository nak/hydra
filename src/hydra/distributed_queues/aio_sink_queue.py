#  Copyright (c) 2025.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import asyncio
import os
import ssl
from contextlib import asynccontextmanager, suppress
from pickle import PickleError
from typing import TypeVar, Generic, AsyncGenerator

import hydra.ssl_contexts
from hydra.distributed_queues.aio_queue_api import AsyncSinkFeed, AsyncConsumer
from hydra.distributed_queues.joinable_queue import SinkJoinableQueue

T = TypeVar('T')
S = TypeVar('S')


class AsyncSinkQueueFeed(AsyncSinkFeed[T]):
    """
    Joinable queue that can be used as a sink to put items to be processed by a remote queue/server.
    """
    _pickle_counter = 0

    def __init__(self, name: str, address: tuple[str, int], sentinel: S,
                 ssl_context: ssl.SSLContext | None = None):
        super().__init__()
        self._name = name
        self._address = address
        self._closed = True
        self._ssl_context = ssl_context
        self._joinable_queue = SinkJoinableQueue[T, S](address=address, sentinel=sentinel)
        self._pickle_task: asyncio.Task | None = None

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
        # ensure copy is registered uniquely
        return {'_name': self._name, '_address': self._address, '_closed': self._closed,
                '_ssl_context': hydra.ssl_contexts.extract_ssl_context_info(self._ssl_context)
                if self._ssl_context else None}

    def __setstate__(self, state) -> None:
        if state.get('_ssl_context'):
            new_context = ssl.SSLContext(state['_ssl_context']['protocol'])
            self._reload_certificates(new_context, state)
        else:
            new_context = None
        self.__class__._pickle_counter += 1
        state['_name'] = f"{state['_name']}-{state['_address'][0]}-{os.getpid()}-{self._pickle_counter}"
        state['_ssl_context'] = new_context
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

    def _reload_certificates(self, ssl_context: ssl.SSLContext, state: dict):
        """
        To be overridden in subclass if needed to apply proper SSL context when pickling/unpickling on
        another host.  This implementation simply loads default certificate
        s on the new context and then applies the SSL context info from the original context,
        """
        ssl_context.load_default_certs()
        hydra.ssl_contexts.rebuild_ssl_context(ssl_context, state['_ssl_context'])

    async def put(self, item: T, timeout: float | None = None) -> None:
        """
        Post data to the joinable queue server.

        Args:
            item: item to put in the queue.
            timeout: The timeout for the transaction.

        """
        if self._pickle_task:
            await self._pickle_task
            self._pickle_task = None
        if self._closed:
            raise RuntimeError("Queue is closed and cannot put items")
        if await self._joinable_queue.transact_async(
                self._address, self._joinable_queue.ACTION_PUT, (item, timeout), timeout=timeout,
                ssl_context=self._ssl_context) != 0:
            raise RuntimeError("Failed to put data to joinable queue server")

    async def close(self):
        """
        Close the connection to the remote queue.
        """
        if self._pickle_task:
            await self._pickle_task
            self._pickle_task = None
        if not self._closed:
            with suppress(ConnectionError):
                await self._joinable_queue.unregister_async(self._name, ssl_context=self._ssl_context)
        self._closed = True


class AsyncSinkQueueConsumer(Generic[T, S], AsyncConsumer[T]):
    """
    Joinable queue that can be used as a sink for items to be processed from multiple remote clients
    """

    def __init__(self, address: tuple[str, int], sentinel: S, size: int = 0, ssl_context: ssl.SSLContext | None = None):
        super().__init__()
        self._address = address
        self._client_ssl_context = ssl_context
        self._joinable_queue = SinkJoinableQueue[T, S](address=address, sentinel=sentinel, size=size)

    @property
    def address(self):
        return self._address

    @asynccontextmanager
    async def start(self, server_ssl_context: ssl.SSLContext | None = None)\
            -> AsyncGenerator["AsyncSinkQueueConsumer[T, S]", None]:
        task = await self._joinable_queue.start_async(server_ssl_context)
        try:
            yield self
        finally:
            self._joinable_queue.shutdown()
        try:
            await asyncio.wait_for(task, timeout=5)
        except asyncio.TimeoutError:
            task.cancel()

    def __getstate__(self) -> dict[str, object]:
        """
        Return only what a client queue needs to connect to the server, not the internal state of the queue/server.
        """
        raise PickleError("AsyncSinkQueueConsumer cannot be pickled as it is not to be duplicated")

    def __setstate__(self, state):
        raise PickleError("AsyncSinkQueueConsumer cannot be (un)pickled as it is not to be duplicated")

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
            self._address, self._joinable_queue.ACTION_GET, payload=timeout, ssl_context=self._client_ssl_context,
            timeout=timeout
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
            self._address, self._joinable_queue.ACTION_JOIN, payload=timeout, ssl_context=self._client_ssl_context,
            timeout=timeout
        ) != 0:
            raise RuntimeError("Failed to join joinable queue server")
