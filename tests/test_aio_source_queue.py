#  Copyright (c) 2026.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import asyncio
from pickle import PickleError

import pytest
import ssl

from hydra.distributed_queues.configuration import SSLCertificatesConfig
from hydra.ssl_contexts import extract_ssl_context_info


@pytest.mark.asyncio
@pytest.mark.asyncio
@pytest.mark.parametrize("ssl_contexts", [(ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER),
                                           ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)), None])
async def test_server_and_client_async_queue(free_port: int, signed_client, signed_server, ssl_contexts: tuple[ssl.SSLContext, ssl.SSLContext]):
    from hydra.distributed_queues.aio_source_queue import AsyncSourceQueueFeed, AsyncSourceQueueConsumer
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
    client_queue = AsyncSourceQueueConsumer[int | str](name="pytest_client", address=(localhost, free_port), ssl_context=client_ssl_context)  # test init without context manager
    async with AsyncSourceQueueFeed[int](address=(localhost, free_port), size=10,
                                        ssl_context=client_ssl_context).start(server_ssl_context) as server_queue:
        async with client_queue:
            for num in range(20):
                if num >= 10:
                    assert await client_queue.get() == num - 10
                    await client_queue.task_started(f'task{num}')
                    await client_queue.task_done(f'task{num}')
                await server_queue.put(num)

            for num in range(10, 20):
                with pytest.raises(asyncio.TimeoutError):
                    await server_queue.join(timeout=0.1)
                assert await client_queue.get() == num
                await client_queue.task_started(f'task{num}')
                await client_queue.task_done(f'task{num}')
                with pytest.raises(RuntimeError):
                    await client_queue.task_done(f'task{num + 1}')

            with pytest.raises(asyncio.queues.QueueEmpty):
                item = await client_queue.get(timeout=0.1)
                assert False, f"Expected QueueEmpty exception, but got item: {item}"


    await server_queue.join(timeout=0.1)


def test_pickle_async_source_queue_feed_with_ssl(free_port: int):
    import pickle
    from hydra.distributed_queues.aio_source_queue import AsyncSourceQueueFeed

    queue = AsyncSourceQueueFeed(address=('127.0.0.1', free_port), size=10)
    queue._server_ssl_context = ssl.create_default_context()
    queue._client_ssl_context = ssl.create_default_context()
    with pytest.raises(PickleError):
        pickle.dumps(queue)


def test_pickle_async_source_queue_consumer_with_ssl(free_port: int):
    import pickle
    from hydra.distributed_queues.aio_source_queue import AsyncSourceQueueConsumer

    queue = AsyncSourceQueueConsumer(name="pytest_client", address=('127.0.0.1', free_port),)
    queue._ssl_context = ssl.create_default_context()
    data = pickle.dumps(queue)
    obj: AsyncSourceQueueConsumer = pickle.loads(data)
    assert obj._name.startswith(queue._name)
    # assert extract_ssl_context_info(obj._ssl_context) == extract_ssl_context_info(queue._ssl_context)
    assert obj._address == queue._address


@pytest.mark.asyncio
@pytest.mark.parametrize("ssl_contexts", [None, (ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER),
                                           ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT))])
async def test_pickling(free_port, signed_client, signed_server, ssl_contexts: tuple[ssl.SSLContext, ssl.SSLContext]):
    import pickle
    from hydra.distributed_queues.aio_source_queue import AsyncSourceQueueFeed, AsyncSourceQueueConsumer
    AsyncSourceQueueConsumer._pickle_counter = 0  # reset counter for test predictability
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

    def reload_certificates(new_context, state):
        # mock to avoid actually trying to load certs during unpickling which can cause issues in test environments
        new_context.check_hostname = False  # Disable for local/IP testing
        new_context.load_verify_locations(cafile=ca_pem)  # Trust the server cert
        new_context.load_cert_chain(certfile=client_pem, keyfile=client_key)
        new_context.load_verify_locations(cafile=ca_pem)  # Trust the server cert
        new_context.load_cert_chain(certfile=server_pem, keyfile=server_key)

    SSLCertificatesConfig.set_reload_callback(reload_certificates)

    async with AsyncSourceQueueFeed[int](address=(localhost, free_port), size=10,
                                                ssl_context=client_ssl_context).start(server_ssl_context) as server_queue:
      async with AsyncSourceQueueConsumer[int](name="pytest_client", address=(localhost, free_port),
                                         ssl_context=client_ssl_context) as client_queue:
        with pytest.raises(PickleError):
            pickle.dumps(server_queue)
        data = pickle.dumps(client_queue)
        new_client_queue: AsyncSourceQueueConsumer = pickle.loads(data)
        new_client_queue2: AsyncSourceQueueConsumer = pickle.loads(data)
        assert not new_client_queue._closed
        assert new_client_queue.name.endswith("-1")
        assert new_client_queue2.name.endswith("-2")
        await server_queue.put(1)
        assert await new_client_queue.get() == 1
