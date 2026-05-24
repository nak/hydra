#  Copyright (c) 2025.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
from abc import abstractmethod, ABC
from typing import Protocol, TypeVar

T = TypeVar('T')


class AsyncConsumer(Protocol[T]):
    """
    Protocol for a consumer that can be used to process messages from a remote queue.
    """

    @abstractmethod
    async def get(self, timeout: float | int | None) -> T | None:
        """
        Get a message from the queue.

        :param timeout: The maximum time to wait for a message.

        :raises TimeoutError: If the operation does not complete in time
        """
        raise NotImplementedError

    async def close(self) -> None:
        """
        Close the connection to the remote queue.
        """
        raise NotImplementedError


class AsyncSinkConsumer(AsyncConsumer[T], ABC):
    """
    A consumer that can be used to process messages from a remote queue and notify when tasks are done. "
    """
    async def join(self) -> None:
        """
        Wait for all tasks to be done.
        """
        raise NotImplementedError


class AsyncFeed(Protocol[T]):
    """
    Protocol for a feed that can be used to put messages into a remote queue.
    """

    @abstractmethod
    async def put(self, item: T, timeout: float | int | None = None) -> None:
        """
        Put a message into the queue.

        :param item: The message to put in the queue.
        :param timeout: The maximum time to wait for the operation to complete.
        """
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """
        Close the connection to the remote queue.
        """
        raise NotImplementedError


class AsyncSourceFeed(AsyncFeed[T]):
    """
    A consumer that can be used to process messages from a remote queue and notify when tasks are done. "
    """

    @abstractmethod
    async def join(self) -> None:
        """
        Wait for all tasks to be done.
        """
        raise NotImplementedError