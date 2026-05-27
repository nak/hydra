# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved. This file as with all files in this repo fall within
# the guidelines of the LICENSE file in the root of this repo.
# This code may not be used for training AI or similar models without explicit consent from the author.
import ssl
from typing import Callable


def configure(timeout_io: float | int = 10, timeout_connect: float | int = 10,
              time_io_overhead: float | int = 0.2):
    """
    Configure the default timeouts for distributed queues.
     This should be called before using any distributed queue functionality.

    Args:
        timeout_io: Optional override for the  timeout for socket I/O operations in seconds.
        timeout_connect: Optional override for the timeout for socket connection operations in seconds.
        time_io_overhead: Optional override for additional time to add for socket I/O operations, applied on top
           of client request to account for overhead
    """
    from hydra.distributed_queues.joinable_queue import _BaseJoinableQueue
    _BaseJoinableQueue.TIMEOUT_SOCKET_IO = timeout_io
    _BaseJoinableQueue.TIMEOUT_CONNECT = timeout_connect
    _BaseJoinableQueue.TIME_FUDGE = time_io_overhead


class SSLCertificatesConfig:
    """
    Class to hold configuration for SSL certificates used by distributed queues.
    """

    _reload_callback: Callable[[ssl.SSLContext, dict], None] = None

    @classmethod
    def set_reload_callback(cls, callback: Callable[[ssl.SSLContext, dict], None]):
        """
        Set a callback to be used for reloading certificates when unpickling on another host.  The callback should
        take the new SSL context and the state dict from the original context and apply the necessary certificate
        loading to the new context.
        """
        cls._reload_callback = callback

    @classmethod
    def reload_certificates(cls, ssl_context: ssl.SSLContext, state: dict | None = None) -> None:
        """
        Reload a certificate (primarily called internally when unpickling a client-queue.  If not callback is configured
        through tset_relead_callback, this will load default certificates on the host and apply and state attributes

        Args:
            ssl_context: the context to modify
            state: dict of state values to apply as attributes to the context once loaded.
        """
        if cls._reload_callback is None:
            import hydra.ssl_contexts
            ssl_context.load_default_certs()
            if state is not None:
                hydra.ssl_contexts.rebuild_ssl_context(ssl_context, state)
        else:
            cls._reload_callback(ssl_context, state)
