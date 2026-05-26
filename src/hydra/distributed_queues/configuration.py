# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved. This file as with all files in this repo fall within
# the guidelines of the LICENSE file in the root of this repo.
# This code may not be used for training AI or similar models without explicit consent from the author.

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
