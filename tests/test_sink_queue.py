#  Copyright (c) 2026.  John Rusnak.  All rights reserved.
#  This code may not be used for training AI or similar models without explicit consent from the author.
import asyncio
import ssl

import pytest

from hydra.ssl_contexts import extract_ssl_context_info


@pytest.mark.asyncio
@pytest.mark.parametrize("ssl_contexts", [(ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER),
                                           ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)), None])
def test_server_and_client_sync_queue(free_port: int, signed_client, signed_server, ssl_contexts: tuple[ssl.SSLContext, ssl.SSLContext]):
    from hydra.distributed_queues.sink_queue import SinkQueueConsumer, SinkQueueFeed

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
    with SinkQueueConsumer[int, str](name="pytest_server", address=(localhost, free_port), sentinel="done", size=10,
                                     ssl_context=client_ssl_context).start(server_ssl_context) as server_queue:
        with SinkQueueFeed(name="pytest_client", address=(localhost, free_port), sentinel="done",
                           ssl_context=client_ssl_context) as client_queue:

            for num in range(20):
                if num >= 10:
                    assert server_queue.get() == num - 10
                client_queue.put(num)

            for num in range(10, 20):
                assert server_queue.get() == num

            with pytest.raises(asyncio.queues.QueueEmpty):
                item = server_queue.get(timeout=0.1)
                if item is not None:
                    assert False, f"Expected QueueEmpty exception, but got item: {item}"

            with pytest.raises(asyncio.TimeoutError):
                server_queue.join(timeout=0.1)

        server_queue.join(timeout=0.1)


def test_pickle_sink_queue_feed_no_ssl(free_port: int):
    import pickle
    from hydra.distributed_queues.sink_queue import SinkQueueFeed

    queue = SinkQueueFeed(name="pytest_client", address=('127.0.0.1', free_port), sentinel="done")
    data = pickle.dumps(queue)
    obj: SinkQueueFeed = pickle.loads(data)
    assert obj._name == queue._name
    assert obj._ssl_context == queue._ssl_context
    assert obj._closed == queue._closed
    assert obj._address == queue._address
    assert obj._ssl_context == None
    queue._ssl_context = ssl.create_default_context()
    data = pickle.dumps(queue)
    obj: SinkQueueFeed = pickle.loads(data)
    assert obj._name == queue._name
    assert extract_ssl_context_info(obj._ssl_context) == extract_ssl_context_info(queue._ssl_context)
    assert obj._closed == queue._closed
    assert obj._address == queue._address


def test_pickle_sink_queue_consumer_with_ssl(free_port: int):
    import pickle
    from hydra.distributed_queues.sink_queue import SinkQueueConsumer

    queue = SinkQueueConsumer(name="pytest_client", address=('127.0.0.1', free_port), sentinel="done")
    queue._client_ssl_context = ssl.create_default_context()
    data = pickle.dumps(queue)
    obj: SinkQueueConsumer = pickle.loads(data)
    assert obj._name == queue._name
    assert isinstance(obj._client_ssl_context, ssl.SSLContext)
    assert extract_ssl_context_info(obj._client_ssl_context) == extract_ssl_context_info(queue._client_ssl_context)
    assert obj._address == queue._address
