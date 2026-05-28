Distributed Queues
==================

.. automodule:: hydra.distributed_queues
   :no-index:


Example: distributed parallelized test system
---------------------------------------------

A common use of a :class:`~hydra.distributed_queues.source_queue.SourceQueueFeed` is to
fan a workload out across many machines. In the figure below a *test-server* populates
a single ``SourceQueueFeed`` with test cases, and multiple
:class:`~hydra.distributed_queues.source_queue.SourceQueueConsumer` clients on remote
worker machines pull the next test from that feed on a first-come-first-served basis.
The feed serializes hand-out, so each test is delivered to exactly one worker.

.. graphviz::
   :alt: A test-server populating a SourceQueueFeed while remote worker clients pull tests first-come-first-served.
   :align: center
   :caption: A ``SourceQueueFeed`` distributing test cases to remote workers in a parallelized test system.

   digraph SourceQueueTestDistribution {
       rankdir=LR;
       splines=spline;
       nodesep=0.5;
       ranksep=1.0;
       bgcolor="white";
       node [fontname="Helvetica", fontsize=11];
       edge [fontname="Helvetica", fontsize=10];

       subgraph cluster_server {
           label="Test Server (host: test-server)";
           labelloc="t";
           fontname="Helvetica-Bold";
           fontsize=12;
           style="rounded,filled";
           color="#3b6ea5";
           fillcolor="#eaf2fb";

           producer [label="Test Producer\n(populates feed)", shape=box,
                     style="rounded,filled", fillcolor="#ffffff", color="#3b6ea5"];

           feed [label="<f0> SourceQueueFeed\n(addr: test-server:5555) | <t1> test_1 | <t2> test_2 | <t3> test_3 | <t4> test_4 | <t5> ... | <tn> test_N",
                 shape=record, style="filled", fillcolor="#fff8dc", color="#b58900"];

           producer -> feed:f0 [label="put(test_k)"];
       }

       subgraph cluster_clients {
           label="Remote Worker Machines";
           labelloc="t";
           fontname="Helvetica-Bold";
           fontsize=12;
           style="rounded,filled";
           color="#2e8b57";
           fillcolor="#eaf7ee";

           client1 [label="Worker A\nSourceQueueConsumer\n(host: worker-a)", shape=box,
                    style="rounded,filled", fillcolor="#ffffff", color="#2e8b57"];
           client2 [label="Worker B\nSourceQueueConsumer\n(host: worker-b)", shape=box,
                    style="rounded,filled", fillcolor="#ffffff", color="#2e8b57"];
           client3 [label="Worker C\nSourceQueueConsumer\n(host: worker-c)", shape=box,
                    style="rounded,filled", fillcolor="#ffffff", color="#2e8b57"];
       }

       feed:t1 -> client1 [label="get() -> test_1", color="#2e8b57"];
       feed:t2 -> client2 [label="get() -> test_2", color="#2e8b57"];
       feed:t3 -> client3 [label="get() -> test_3", color="#2e8b57"];
       feed:t4 -> client1 [label="get() -> test_4", color="#2e8b57", style=dashed];

       caption [shape=plaintext, fontsize=10,
                label="First-come-first-served"];
       { rank=sink; caption; }
   }

Public API
----------

Source Queues
~~~~~~~~~~~~~

.. automodule:: hydra.distributed_queues.source_queue
   :members:
   :show-inheritance:

.. automodule:: hydra.distributed_queues.aio_source_queue
   :members:
   :show-inheritance:

Sink Queues
~~~~~~~~~~~

.. automodule:: hydra.distributed_queues.sink_queue
   :members:
   :show-inheritance:

.. automodule:: hydra.distributed_queues.aio_sink_queue
   :members:
   :show-inheritance:

Internals
---------

Configuration
~~~~~~~~~~~~~

.. automodule:: hydra.distributed_queues.configuration
   :members:
   :show-inheritance:
