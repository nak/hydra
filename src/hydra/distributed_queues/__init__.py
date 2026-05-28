"""
This module contains the logic for distributed queues.  They are based on a single server hosting a common queue,
with multiple (remote) client proxies to interact with it..  The types of queues supported are:

#. *Source queues*: a source queue acts as a single queue for a client to populate, with remote worker clients pulling
   from the queue on a first-come-first-serve basis.  Each item put into the queue is served to only one worker client.
   The queue is a joinable queue, meaning that workers signal that a task ist started and when a task is completed to
   the server.  A call to the server queue's join method waits until all tasks are done for each item that has been
   served.
#. *Sink queues*: A sink queue follows the opposite data flow.  The server queue is a sink of data items that the
   server pulls from, with remote worker clients pushing items into the queue.  This queue is also joinable, with
   each worker registering with the queue to gain access, and
   a call to join the queue from the server waiting until all clients have unregistered.

Both async and non-async classes are provided.  Also, client-queue class instances are pickleable, allowing them
to be readily passed to remote processes. (For example, using a RPC package like *bantam*)

The queues are socket-based and can use SSL communications.  This package does not dictate how certificate
files or host configurations are
managed when access is required across multiple hosts. Configuration to (re)load certificates is achieved
through a callback provided by the user.

As an example, the server code might look like:

>>> from hydra.distributed_queues.aio_source_queue import AsyncSourceQueueFeed
...
... async def populate():
...     host, port = ...
...     async with AsyncSourceQueueFeed[str](address=(host, port), size=100).start() as server_queue:
...         for task in ['task1', 'task2', 'task3']:
...             await server_queue.put(task)
...         await server_queue.join(timeout=1.0)

While example client code looks like:

>>> from hydra.distributed_queues.aio_source_queue import AsyncSourceQueueConsumer
...
... async def process_task():
...    ...
...
... async def process_queue():
...     host, port = ...
...     try:
...         async with AsyncSourceQueueConsumer(address=(host, port)) as client_queue:
...             while task := await client_queue.get(timeout=1.0):
...                 await process_task(task)
"""
