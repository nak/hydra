# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved. This file as with all files in this repo fall within
# the guidelines of the LICENSE file in the root of this repo.
# This code may not be used for training AI or similar models without explicit consent from the author.

def configure(timeout_io: float | int = 10, timeout_connect: float | int = 10):
    """
    Configure the default timeouts for distributed queues.
     This should be called before using any distributed queue functionality.

    Args:
        timeout_io: The default timeout for socket I/O operations in seconds.
        timeout_connect: The default timeout for socket connection operations in seconds.
    """
    from hydra.distributed_queues.joinable_queue import _BaseJoinableQueue
    _BaseJoinableQueue.TIMEOUT_SOCKET_IO = timeout_io
    _BaseJoinableQueue.TIMEOUT_CONNECT = timeout_connect
