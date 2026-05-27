# Copyright (c) 2026, all rights reserved
import asyncio
import logging
import os
import pickle
import socket
import ssl
import struct
import sys
import threading
import time
from contextlib import suppress
from copy import copy
from typing import Generic, TypeVar

from hydra.exceptions import OperationCanceledError

# Generic Type for queue
T = TypeVar('T')
# Sentinel Type:
S = TypeVar('S')

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("PYTEST_MPROC_LOG_LEVEL", "WARNING").upper())
logger.addHandler(logging.StreamHandler())


def _recv_all(sock, n):
    """Helper function to receive exactly n bytes or return None if EOF is hit."""
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            raise ConnectionError("Socket connection closed while trying to receive data."
                                  "If  you are using SSL, ensure that your are passing ssl context for server on start"
                                  " and client ssl context on __init__")
        data.extend(packet)
    return data


class _BaseJoinableQueue(Generic[T, S]):
    """
    Base class for joinable queues.
    """
    TIMEOUT_CONNECT = 10  # seconds
    TIMEOUT_SOCKET_IO = 10  # seconds
    TIME_FUDGE = 0.2  # fudge to add to socket timeout to account for I/O overhead
    _MAX_PACKET_SIZE = 1024  # bytes
    _MAX_CLIENT_Q_SIZE = 100  # max depth of client queues

    ACTION_GET = "GET"
    ACTION_PUT = "PUT"
    ACTION_JOIN = "JOIN"
    ACTION_TASK_STARTED = "TASK_STARTED"
    ACTION_TASK_DONE = "TASK_DONE"
    ACTION_REGISTER = "REGISTER"
    ACTION_UNREGISTER = "UNREGISTER"
    ACTION_WAIT_CLIENTS = "WAIT_CLIENTS"

    def __init__(self, address: tuple[str, int], size: int, sentinel: S):
        self._address = address
        self._clients: asyncio.Queue[str] = asyncio.Queue(self._MAX_CLIENT_Q_SIZE)
        self._client_ids: set[str] = set()
        self._shutdown_sem = threading.Semaphore(0)
        self._cleanup_sem: threading.Semaphore | None = None
        self._tasks_in_progress: set[T] = set()
        self._size = size
        self._sentinel = sentinel

    def client_count(self):
        """
        Returns the number of connected clients.
        """
        return len(self._client_ids)

    @property
    def address(self):
        """
        Returns the address of this joinable queue server.
        """
        return self._address

    async def _handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                              queue: asyncio.Queue[T | S]):
        """
        Handle incoming requests from clients.

        :param reader: The stream reader for incoming data.
        :param writer: The stream writer for sending data.
        """
        packet_size = struct.unpack("!L", await reader.readexactly(4))[0]
        request_bytes = await reader.readexactly(packet_size)
        if not request_bytes:
            data = pickle.dumps(RuntimeError("Empty request received."))
            writer.write(struct.pack("!L", len(data)))
            writer.write(data)
            await writer.drain()
            writer.close()
            return
        action, payload = pickle.loads(request_bytes)
        response_bytes = None
        try:
            response_bytes = await self._take_action(queue, action, payload)
        except asyncio.CancelledError:
            if action == self.ACTION_GET:
                response_bytes = pickle.dumps(asyncio.QueueEmpty("Server task canceled; no item to service in queue"))
            else:
                response_bytes = pickle.dumps(
                    OperationCanceledError("Server task cancelled; no item to service in queue")
                )
        except pickle.UnpicklingError as upe:
            print(f"Unable to unpickle request: {upe}", file=sys.stderr)
            response_bytes = pickle.dumps(RuntimeError(f"Unable to unpickle request: {upe}"))
        except (asyncio.exceptions.TimeoutError, TimeoutError) as te:
            if action == self.ACTION_GET:
                response_bytes = pickle.dumps(asyncio.QueueEmpty("Timeout waiting for item in queue"))
            elif action == self.ACTION_PUT:
                response_bytes = pickle.dumps(asyncio.QueueFull("Timeout waiting for item to be put in queue"))
            else:
                response_bytes = pickle.dumps(te)
        except asyncio.QueueEmpty as qe:
            response_bytes = pickle.dumps(qe)
        except Exception as e:
            response_bytes = pickle.dumps(e)
        finally:
            if response_bytes is None:
                response_bytes = pickle.dumps(RuntimeError("No response generated for request."))
            writer.write(struct.pack("!L", len(response_bytes)))
            writer.write(response_bytes)
            await writer.drain()
            writer.close()
            await writer.wait_closed()

    async def _take_action(self, queue: asyncio.Queue[T | S], action: str, payload: T | S) -> bytes:
        """
        Take action based on the request action and payload.

        :param action: The action to perform (GET, PUT, JOIN, TASK_DONE, REGISTER).
        :param payload: The payload associated with the action.

        :returns: The response bytes to send back to the client.
        """
        match action:
            case self.ACTION_GET:
                timeout = payload
                no_wait = timeout == 0
                if no_wait:
                    # generally called after a join operation when no more items are expected to be put in the queue
                    item = queue.get_nowait()
                elif timeout is not None:
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=timeout)
                    except asyncio.TimeoutError:
                        raise asyncio.queues.QueueEmpty("Timeout waiting for item in queue")
                else:
                    item = await queue.get()
                response_bytes = pickle.dumps(item)
            case self.ACTION_PUT:
                try:
                    item, timeout = payload
                except TypeError:
                    item = payload
                    timeout = None
                await asyncio.wait_for(queue.put(item), timeout=timeout)
                response_bytes = b'\x00'
            case self.ACTION_JOIN:
                if payload is not None and not isinstance(payload, (float, int)):
                    response_bytes = pickle.dumps(TypeError("Expected float or int timeout value for join operation."))
                else:
                    timeout = payload
                    await asyncio.wait_for(queue.join(), timeout=timeout)
                    response_bytes = b'\x00'
            case self.ACTION_TASK_DONE:
                if payload is not None and payload not in self._tasks_in_progress:
                    raise RuntimeError(f"Received task_done for item {payload} not in progress:")
                elif payload is not None:
                    self._tasks_in_progress.remove(payload)
                queue.task_done()
                response_bytes = b'\x00'
            case self.ACTION_TASK_STARTED:
                if payload is not None and payload in self._tasks_in_progress:
                    raise RuntimeError(f"Received task_started for item already in progress: {payload}")
                elif payload is not None:
                    self._tasks_in_progress.add(payload)
                response_bytes = b'\x00'
            case self.ACTION_REGISTER:
                # Register a new client
                if payload in self._client_ids:
                    response_bytes = pickle.dumps(ValueError(f"Client {payload} already registered."))
                else:
                    await self._clients.put(payload)
                    self._client_ids.add(payload)
                    response_bytes = b'\x00'
            case self.ACTION_UNREGISTER:
                # Unregister a client
                if payload not in self._client_ids:
                    response_bytes = pickle.dumps(KeyError(f"Client {payload} not registered."))
                else:
                    self._clients.get_nowait()
                    self._client_ids.remove(payload)
                    self._clients.task_done()
                    response_bytes = b'\x00'
            case self.ACTION_WAIT_CLIENTS:
                if not isinstance(payload, (float | int | None)):
                    response_bytes = pickle.dumps(TypeError("Expected float or int timeout value for wait operation."))
                else:
                    timeout = payload
                    await asyncio.wait_for(self._clients.join(), timeout=timeout)
                    response_bytes = b'\x00'
            case _:
                raise ValueError(f"Unknown action: {action}")
        return response_bytes

    def close(self):
        return self.shutdown()

    def shutdown(self):
        self._shutdown_sem.release()
        if self._cleanup_sem is not None:
            self._cleanup_sem.release()
            self._cleanup_sem = None

    async def _serve(self, start_sem: threading.Semaphore | asyncio.Semaphore, context: ssl.SSLContext | None):
        """
        Start the joinable queue server.
        """
        queue = asyncio.Queue[T | S](self._size)
        if self._cleanup_sem is None:
            self._cleanup_sem = threading.Semaphore(0)

        async def handle_request_bound(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            await self._handle_request(reader, writer, queue)

        transport = await asyncio.start_server(handle_request_bound, host=self._address[0], port=self._address[1],
                                               ssl=context)
        start_sem.release()
        try:
            # Must implement polling on threading.Semaphore as an asyncio.Semaphore cannot be used across different
            # threads
            while not self._shutdown_sem.acquire(blocking=False):
                await asyncio.sleep(1)
            # drain the queue
            while not queue.empty():
                item = queue.get_nowait()
                if item != self._sentinel:
                    logger.error(">> WARNING: Item still in queue during shutdown: %s", item)
                queue.task_done()
        finally:
            transport.close()
            await transport.wait_closed()
            if self._cleanup_sem is not None:
                self._cleanup_sem.release()

    def start(self, ssl_context: ssl.SSLContext | None) -> threading.Thread:
        """
        Start the joinable queue server in a separate thread.
        """
        start_sem = threading.Semaphore(0)
        thread = threading.Thread(target=asyncio.run,
                                  args=(self._serve(start_sem, ssl_context),))
        thread.start()
        start_sem.acquire(blocking=True)
        return thread

    async def start_async(self, ssl_context: ssl.SSLContext | None) -> asyncio.Task:
        """
        Start the joinable queue server in a separate thread.
        """
        start_sem = asyncio.Semaphore(0)
        task = asyncio.create_task(self._serve(start_sem, ssl_context))
        await start_sem.acquire()
        return task

    @classmethod
    def transact(cls, address: tuple[str, int], action: str, payload: T | S, ssl_context: ssl.SSLContext | None,
                 timeout: float | None = None) -> int | T | S:
        """
        Perform a transaction with the joinable queue server.

        :param address: The address of the server (host, port).
        :param action: The action to perform (GET, PUT, JOIN, TASK_DONE, REGISTER).
        :param payload: The payload associated with the action.
        :param timeout: The timeout for the transaction.
        :param ssl_context: optional SSL context

        :returns: The response bytes from the server.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if timeout is not None:
            sock.settimeout(timeout + cls.TIME_FUDGE)
        try:
            sock.connect(address)
        except TimeoutError:
            raise TimeoutError(f"Timeout connecting to {address} after {timeout} seconds")
        if ssl_context is not None:
            with ssl_context.wrap_socket(sock, server_hostname=address[0]) as ssock:
                result = cls.transact_sock(action, payload, sock=ssock, timeout=timeout)
        else:
            result = cls.transact_sock(action, payload, sock=sock, timeout=timeout)
        return result

    @classmethod
    def transact_sock(cls,  action: str, payload: T | S,  sock: socket.socket, timeout: int | float | None)\
            -> int | T | S:
        result_bytes = b''
        first = True
        start = time.monotonic()
        while first or timeout is None or time.monotonic() - start <= timeout + cls.TIME_FUDGE:
            first = False
            try:
                data = pickle.dumps((action, payload))
                sock.sendall(struct.pack("!L", len(data)))
                sock.sendall(data)
                size = struct.unpack("!L", _recv_all(sock, 4))[0]
                result_bytes = _recv_all(sock, size)
            except (asyncio.exceptions.TimeoutError, TimeoutError):
                if timeout is not None and time.monotonic() - start > timeout + cls.TIME_FUDGE:
                    raise TimeoutError("Timeout waiting for response from server")
            else:
                with suppress(Exception):
                    sock.shutdown(socket.SHUT_WR)
                with suppress(Exception):
                    sock.shutdown(socket.SHUT_RD)
                sock.close()
                break
        if timeout is not None and time.monotonic() - start > timeout + cls.TIME_FUDGE:
            raise TimeoutError("Timeout waiting for response from server")
        return cls._process_response(result_bytes, action)

    @classmethod
    def _process_response(cls, result_bytes: bytes, action: str) -> int | T | S:
        if len(result_bytes) > 1:
            result = pickle.loads(result_bytes)
            if isinstance(result, pickle.UnpicklingError):
                raise RuntimeError("Failed to unpickle response from server") from result
            elif isinstance(result, Exception):
                raise copy(result) from result
            return result
        elif action in (cls.ACTION_JOIN, ):
            return 0
        elif len(result_bytes) == 1:
            return result_bytes[0]
        elif action in (cls.ACTION_GET, ):
            raise asyncio.QueueEmpty()
        else:
            raise ConnectionError("No response received from server")

    async def transact_async(self, address: tuple[str, int], action: str, payload: T | S,
                             timeout: float | None = TIMEOUT_SOCKET_IO,
                             ssl_context: ssl.SSLContext | None = None) -> int | T | S:
        """
        Perform an asynchronous transaction with the joinable queue server.

        :param address: The address of the server (host, port).
        :param action: The action to perform (GET, PUT, JOIN, TASK_DONE, REGISTER).
        :param payload: The payload associated with the action.
        :param timeout: The timeout for the transaction.

        :returns: The response from the server.
        """
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(*address, ssl=ssl_context, server_hostname=address[0] if ssl_context else None),
                timeout=timeout
            )
        except asyncio.exceptions.TimeoutError:
            print("Timeout trying to connect to server", file=sys.stderr)
            raise
        start = time.monotonic()
        first = True
        while first or timeout is None or time.monotonic() - start <= timeout + self.TIME_FUDGE:
            first = False
            try:
                return await self.transact_sock_async(action, payload, timeout=timeout,
                                                      reader=reader, writer=writer)
            except (asyncio.exceptions.TimeoutError, TimeoutError):
                if time.monotonic() - start > timeout:
                    raise
        raise TimeoutError("Timeout waiting for response from server")

    async def transact_sock_async(self, action: str, payload: T | S, timeout: float | None,
                                  reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> int | T | S:
        try:
            data = pickle.dumps((action, payload))
            writer.write(struct.pack("!L", len(data)))
            writer.write(data)
            await writer.drain()
            result_size = struct.unpack("!L", await reader.readexactly(4))[0]
            if timeout is not None:
                result_bytes = await asyncio.wait_for(reader.readexactly(result_size), timeout=timeout)
            else:
                result_bytes = await reader.readexactly(result_size)
            return self._process_response(result_bytes, action)
        except (asyncio.exceptions.TimeoutError, TimeoutError):
            raise asyncio.exceptions.TimeoutError("Timeout waiting for response from server")
        finally:
            with suppress(Exception):
                writer.close()
                await writer.wait_closed()

    def register(self, client_id: str, ssl_context: ssl.SSLContext | None) -> None:
        """
        Register a new client with the joinable queue server.

        :raises RuntimeError: if the server returns an error
        """
        if self.transact(self._address, self.ACTION_REGISTER, client_id, timeout=self.TIMEOUT_SOCKET_IO,
                         ssl_context=ssl_context) != 0:
            raise RuntimeError(f"Failed to register client {client_id} with joinable queue server")

    def unregister(self, client_id: str,  ssl_context: ssl.SSLContext | None) -> int:
        """
        Register a new client with the joinable queue server.

        :raises RuntimeError: if the server returns an error
        """
        if self.transact(self._address, self.ACTION_UNREGISTER, client_id, timeout=self.TIMEOUT_SOCKET_IO,
                         ssl_context=ssl_context) != 0:
            raise RuntimeError(f"Failed to unregister client {client_id} with joinable queue server")
        return 0

    async def unregister_async(self, client_id: str,  ssl_context: ssl.SSLContext | None) -> None:
        """
        Register a new client with the joinable queue server.

        :raises RuntimeError: if the server returns an error
        """
        if await self.transact_async(
            self._address, self.ACTION_UNREGISTER, client_id, timeout=self.TIMEOUT_SOCKET_IO,
            ssl_context=ssl_context
        ) != 0:
            raise RuntimeError(f"Failed to register client {client_id} with joinable queue server")

    async def register_async(self, client_id: str, ssl_context: ssl.SSLContext | None) -> None:
        """
        Register a new client with the joinable queue server.

        :raises RuntimeError: if the server returns an error
        """
        if await self.transact_async(
            self._address, self.ACTION_REGISTER, client_id, timeout=self.TIMEOUT_SOCKET_IO,
            ssl_context=ssl_context
        ) != 0:
            raise RuntimeError(f"Failed to register client {client_id} with joinable queue server")


class SinkJoinableQueue(_BaseJoinableQueue[T, S]):
    """
    Joinable queue that can be used as a sink for items to be processed by multiple clients.
    """

    def __init__(self, address: tuple[str, int], sentinel: S, size: int = 0):
        super().__init__(address, size, sentinel)
        self._waiting_on_clients = False
        self._address = address

    async def _take_action(self, queue: asyncio.Queue[T | S], action: str, payload: float | int | T | S) -> bytes:
        if action == self.ACTION_JOIN:
            self._waiting_on_clients = True
            response_bytes = await super()._take_action(queue, self.ACTION_WAIT_CLIENTS, payload)
            await queue.put(self._sentinel)
        elif action == self.ACTION_UNREGISTER:
            response_bytes = await super()._take_action(queue, action, payload)
        else:
            response_bytes = await super()._take_action(queue, action, payload)
        return response_bytes


class SourceJoinableQueue(_BaseJoinableQueue[T, S]):
    """
    Joinable queue that can be used as a source for items to be processed by multiple clients.
    """

    def __init__(self, address: tuple[str, int], size: int = 0):
        super().__init__(address, size, None)
        self._finalizing = False

    async def _take_action(self, queue: asyncio.Queue[T | S], action: str, payload: float | int | T | S) -> bytes:
        if action == self.ACTION_GET:
            if self._finalizing:
                item = queue.get_nowait()
                response_bytes = pickle.dumps(item)
            else:
                response_bytes = await super()._take_action(queue, action, payload)
        elif action == self.ACTION_JOIN:
            self._finalizing = True
            response_bytes = await super()._take_action(queue, action, payload)
        else:
            response_bytes = await super()._take_action(queue, action, payload)
        return response_bytes
