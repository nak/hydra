#  Copyright (c) 2026.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import asyncio
import ssl
from pickle import PickleError

import pytest

from hydra.ssl_contexts import extract_ssl_context_info


@pytest.mark.asyncio
@pytest.mark.parametrize("ssl_contexts", [(ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER),
                                           ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)), None])
def test_server_and_client_sync_queue(free_port: int, signed_client, signed_server, ssl_contexts: tuple[ssl.SSLContext, ssl.SSLContext]):
    from hydra.distributed_queues.source_queue import SourceQueueFeed, SourceQueueConsumer

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
    with SourceQueueFeed(address=(localhost, free_port), size=10, ssl_context=client_ssl_context)\
            .start(server_ssl_context) as server_queue:
      with SourceQueueConsumer(name="pytest_client", address=(localhost, free_port),
                               ssl_context=client_ssl_context) as client_queue:

        for num in range(20):
            if num >= 10:
                assert client_queue.get() == num - 10
                client_queue.task_started(f'task{num}')
                client_queue.task_done(f'task{num}')
            server_queue.put(num)

        for num in range(10, 20):
            with pytest.raises(asyncio.TimeoutError):
                server_queue.join(timeout=0.1)
            assert client_queue.get() == num
            client_queue.task_started(f'task{num}')
            client_queue.task_done(f'task{num}')
            with pytest.raises(RuntimeError):
                client_queue.task_done(f'task{num + 1}')

        with pytest.raises(asyncio.queues.QueueEmpty):
            item = client_queue.get(timeout=0.1)
            assert False, f"Expected QueueEmpty exception, but got item: {item}"


    server_queue.join(timeout=0.1)


def test_pickle_source_queue_feed_no_ssl(free_port: int):
    import pickle
    from hydra.distributed_queues.source_queue import SourceQueueFeed

    queue = SourceQueueFeed(address=('127.0.0.1', free_port), size=10)
    with pytest.raises(PickleError):
        data = pickle.dumps(queue)


def test_pickle_source_queue_consumer_with_ssl(free_port: int):
    import pickle
    from hydra.distributed_queues.source_queue import SourceQueueConsumer

    queue = SourceQueueConsumer(name='pytest-consumer', address=('127.0.0.1', free_port))
    queue._ssl_context = ssl.SSLContext()
    queue._ssl_context.load_default_certs()
    data = pickle.dumps(queue)
    obj: SourceQueueConsumer = pickle.loads(data)
    assert isinstance(obj._ssl_context, ssl.SSLContext)
    assert extract_ssl_context_info(obj._ssl_context) == extract_ssl_context_info(queue._ssl_context)
    assert obj._address == queue._address
