"""
This module contains the logic for distributed queues.  They are based on a single server with multiple (remote)
clients.  The type of queues supported are:

1. Source queues: these are queues that can be used to consume items from a single remote queue/server.
   The AsyncSourceQueueFeed class acts as a server, providing the interface to put items in the queue.
   The AsyncSourceQueueConsumer class acts as a client, providing the interface to pull the next item from the queue.
   These queues are joinable, meaning that the client can notify the server when a task is started and when it is done.
2. Sink queues: these are queues that can be used to post items to a single remote queue/server.  The
   AsyncSinkQueueConsumer class acts as a server, providing the interface to pull items from the queue
   The AsyncSinkQueueFeed class acts as a client, providing the interface to put items in the queue.  Clients must
   connect (register) with the server to be usable, and disconnected (unregistered) once done. (This can
   be handled automatically by using the queue as a context manager).  The server queue is joinable, and once
   join() is called, the server will wait until all clients are unregistered before returning.

Both async and non-async classes are provided.  Also, client-queue class instances are pickleable, allowing them
to be readily passed to remote processes.

The queues can uss SSL communications, with configuration to (re)load certificates through a callback in a user-defined
manner.  This package does not dictate how certificate files or host configurations are managed when distributing across
multiple hosts.  Port configurations are determined internally and are randomly assigned an available port on the host.
"""
