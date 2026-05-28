Hydra
=====

Hydra is a general-purpose package providing distributed capabilities for applications spread across remote hosts.
It handles the coordination, communication, and data-passing concerns common to distributed systems, so that
application code can focus on logic rather than networking plumbing.

Current capabilities:

- **Distributed Queues** — source and sink queue pairs allowing items to be produced and consumed across host boundaries, with optional SSL and join semantics.

More modules will be added here as the package grows.

.. toctree::
   :maxdepth: 1
   :caption: Modules:

   distributed_queues
