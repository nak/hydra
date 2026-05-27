#  Copyright (c) 2025.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
from abc import abstractmethod
from typing import Protocol, TypeVar

T = TypeVar('T')


class Consumer(Protocol[T]):
    """
    Protocol for a consumer that can be used to process messages from a remote queue.
    """

    @abstractmethod
    def get(self, timeout: float | int | None) -> T | None:
        """
        Get a message from the queue.

        :param timeout: The maximum time to wait for a message.

        :raises TimeoutError: If the operation does not complete in time
        """
        raise NotImplementedError

    def close(self) -> None:
        """
        Close the connection to the remote queue.
        """
        raise NotImplementedError


class SinkConsumer(Consumer[T]):
    """
    A consumer that can be used to process messages from a remote queue and notify when tasks are done. "
    """
    @abstractmethod
    def join(self) -> None:
        """
        Wait for all tasks to be done.
        """
        raise NotImplementedError


class Feed(Protocol[T]):
    """
    Protocol for a feed that can be used to put messages into a remote queue.
    """

    @abstractmethod
    def put(self, item: T, timeout: float | int | None = None) -> None:
        """
        Put a message into the queue.

        :param item: The message to put in the queue.
        :param timeout: The maximum time to wait for the operation to complete.
        """
        raise NotImplementedError


class SourceFeed(Feed[T]):
    """
    A consumer that can be used to process messages from a remote queue and notify when tasks are done. "
    """

    @abstractmethod
    def join(self) -> None:
        """
        Wait for all tasks to be done.
        """
        raise NotImplementedError
