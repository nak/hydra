#  Copyright (c) 2026.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import asyncio
import ssl
from pickle import PickleError
from unittest.mock import patch

import pytest

from hydra.ssl_contexts import extract_ssl_context_info


@pytest.mark.asyncio
@pytest.mark.parametrize("ssl_contexts", [(ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER),
                                           ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)), None])
async def test_server_and_client_async_queue(free_port: int, signed_client, signed_server, ssl_contexts: tuple[ssl.SSLContext, ssl.SSLContext]):
    from hydra.distributed_queues.aio_sink_queue import AsyncSinkQueueConsumer, AsyncSinkQueueFeed

    localhost = '127.0.0.1'
    server_pem, server_key, ca_pem = signed_server
    client_pem, client_key, _ = signed_client
    server_ssl_context, client_ssl_context = ssl_contexts if ssl_contexts else (None, None)
    if client_ssl_context:
        client_ssl_context.check_hostname = False  # Disable for local/IP testing
        client_ssl_context.load_verify_locations(cafile=ca_pem)  # Trust the server cert
        client_ssl_context.load_cert_chain(certfile=client_pem, keyfile=client_key)
        server_ssl_context.load_verify_locations(cafile=ca_pem)  # Trust the server cert
        server_ssl_context.load_cert_chain(certfile=server_pem, keyfile=server_key)
    async with AsyncSinkQueueConsumer[int, str](address=(localhost, free_port), sentinel="done", size=10,
                                                ssl_context=client_ssl_context).start(server_ssl_context) as server_queue:
      async with AsyncSinkQueueFeed[int](name="pytest_client", address=(localhost, free_port), sentinel="done",
                                         ssl_context=client_ssl_context) as client_queue:

        for num in range(20):
            if num >= 10:
                assert await server_queue.get() == num - 10
            await client_queue.put(num)

        for num in range(10, 20):
            assert await server_queue.get() == num

        with pytest.raises(asyncio.queues.QueueEmpty):
            await server_queue.get(timeout=1.1)

        with pytest.raises(asyncio.TimeoutError):
            await server_queue.join(timeout=0.1)

      await server_queue.join(timeout=0.1)


@pytest.mark.asyncio
@pytest.mark.parametrize("ssl_contexts", [(ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER),
                                           ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)), None])
async def test_server_and_client_async_queue_with_delay(free_port: int, signed_client, signed_server, ssl_contexts: tuple[ssl.SSLContext, ssl.SSLContext]):
    from hydra.distributed_queues.aio_sink_queue import AsyncSinkQueueConsumer, AsyncSinkQueueFeed

    localhost = '127.0.0.1'
    server_pem, server_key, ca_pem = signed_server
    client_pem, client_key, _ = signed_client
    server_ssl_context, client_ssl_context = ssl_contexts if ssl_contexts else (None, None)
    if client_ssl_context:
        client_ssl_context.check_hostname = False  # Disable for local/IP testing
        client_ssl_context.load_verify_locations(cafile=ca_pem)  # Trust the server cert
        client_ssl_context.load_cert_chain(certfile=client_pem, keyfile=client_key)
        server_ssl_context.load_verify_locations(cafile=ca_pem)  # Trust the server cert
        server_ssl_context.load_cert_chain(certfile=server_pem, keyfile=server_key)
    async def mock_get(self, *args, **kwargs):
        # will simulate server-side timeout based on logic below of 1 second timeout (leading to QueueEmpty),
        await asyncio.sleep(5)
        return await asyncio.queues.Queue.get(self, *args, **kwargs)

    with patch("asyncio.queues.Queue.get", mock_get):
        async with AsyncSinkQueueConsumer[int, str](address=(localhost, free_port), sentinel="done", size=10,
                                                    ssl_context=client_ssl_context).start(server_ssl_context) as server_queue:
            async with AsyncSinkQueueFeed[int](name="pytest_client", address=(localhost, free_port), sentinel="done",
                                               ssl_context=client_ssl_context) as client_queue:

                await client_queue.put(1)
                with pytest.raises(asyncio.QueueEmpty):
                    await server_queue.get(timeout=1)

                with pytest.raises(asyncio.TimeoutError):
                    await server_queue.join(timeout=0.1)

            await server_queue.join(timeout=0.1)


def test_pickle_async_sink_queue_feed_no_ssl(free_port: int):
    import pickle
    from hydra.distributed_queues.aio_sink_queue import AsyncSinkQueueFeed

    queue = AsyncSinkQueueFeed(name="pytest_client", address=('127.0.0.1', free_port), sentinel="done")
    data = pickle.dumps(queue)
    obj: AsyncSinkQueueFeed = pickle.loads(data)
    assert obj._name.startswith(queue._name)
    assert obj._ssl_context == queue._ssl_context
    assert obj._closed == queue._closed
    assert obj._address == queue._address


def test_pickle_async_sink_queue_feed_with_ssl(free_port: int):
    import pickle
    from hydra.distributed_queues.aio_sink_queue import AsyncSinkQueueFeed

    queue = AsyncSinkQueueFeed(name="pytest_client", address=('127.0.0.1', free_port), sentinel="done")
    queue._ssl_context = ssl.create_default_context()
    data = pickle.dumps(queue)
    obj: AsyncSinkQueueFeed = pickle.loads(data)
    assert obj._name.startswith(queue._name)
    assert extract_ssl_context_info(obj._ssl_context) == extract_ssl_context_info(queue._ssl_context)
    assert obj._closed == queue._closed
    assert obj._address == queue._address


@pytest.mark.asyncio
@pytest.mark.parametrize("ssl_contexts", [None, (ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER),
                                           ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT))])
async def test_pickling(free_port, signed_client, signed_server, ssl_contexts: tuple[ssl.SSLContext, ssl.SSLContext]):
    import pickle
    from hydra.distributed_queues.aio_sink_queue import AsyncSinkQueueConsumer, AsyncSinkQueueFeed
    AsyncSinkQueueFeed._pickle_counter = 0  # reset counter for test predictability
    localhost = '127.0.0.1'
    server_pem, server_key, ca_pem = signed_server
    client_pem, client_key, _ = signed_client
    server_ssl_context, client_ssl_context = ssl_contexts if ssl_contexts else (None, None)
    if client_ssl_context:
        client_ssl_context.check_hostname = False  # Disable for local/IP testing
        client_ssl_context.load_verify_locations(cafile=ca_pem)  # Trust the server cert
        client_ssl_context.load_cert_chain(certfile=client_pem, keyfile=client_key)
        server_ssl_context.load_verify_locations(cafile=ca_pem)  # Trust the server cert
        server_ssl_context.load_cert_chain(certfile=server_pem, keyfile=server_key)

    def mock_reload_certificates(self, new_context, state):
        # mock to avoid actually trying to load certs during unpickling which can cause issues in test environments
        new_context.check_hostname = False  # Disable for local/IP testing
        new_context.load_verify_locations(cafile=ca_pem)  # Trust the server cert
        new_context.load_cert_chain(certfile=client_pem, keyfile=client_key)
        new_context.load_verify_locations(cafile=ca_pem)  # Trust the server cert
        new_context.load_cert_chain(certfile=server_pem, keyfile=server_key)

    AsyncSinkQueueFeed._reload_certificates = mock_reload_certificates  # patch the method for testing to avoid cert loading issues

    async with AsyncSinkQueueConsumer[int, str](address=(localhost, free_port), sentinel="done", size=10,
                                                ssl_context=client_ssl_context).start(server_ssl_context) as server_queue:
      async with AsyncSinkQueueFeed[int](name="pytest_client", address=(localhost, free_port), sentinel="done",
                                         ssl_context=client_ssl_context) as client_queue:
        with pytest.raises(PickleError):
            pickle.dumps(server_queue)
        data = pickle.dumps(client_queue)
        new_client_queue: AsyncSinkQueueFeed = pickle.loads(data)
        new_client_queue2: AsyncSinkQueueFeed = pickle.loads(data)
        assert not new_client_queue._closed
        assert new_client_queue.name.endswith("-1")
        assert new_client_queue2.name.endswith("-2")
        await new_client_queue.put(1)
        assert await server_queue.get() == 1