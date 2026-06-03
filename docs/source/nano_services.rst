Nano Services
=============

.. automodule:: hydra.nano_services
   :no-index:


Real-World Application: device reservation in a distributed test system
-----------------------------------------------------------------------

To provide a more concreate picture, let's consider extending the distributed test example found
in the Distribute Queues sub-module section.  Let's use nano-services to support a device
reservation system for test execution: the tests run *against* a pool of devices --
hardware nodes, virtual environments, simulators, etc. -- that are spread across multiple
host machines, and each device can only be running one test at any given time.
The reservation system's job is to pick an available device from the pool and reserve it for a
test, then later release it when the test finishes so it can be reserved again.

In this architecture, every host that owns devices runs a :mod:`hydra.nano_services`
**agent** that is the source of truth for the availability of its local devices: a device
is "available" only if its host's agent says so. A central **reservation system** holds an
*approximate* registry of device state aggregated from all hosts, and uses that approximation to
pick candidates to reserve. Because the registry is only an approximation, reservation is a
two-step protocol with built-in self-healing:

#. The reservation system consults the registry and picks a candidate device.
#. It calls the ``reserve(device_id)`` ``@web_api`` on the owning host's agent.

The agent then does one of two things, depending on whether its own ground-truth state
matches the registry's approximation for that device:

* **In sync** (agent agrees the device is available) -- the agent reserves the device, marks
  it busy locally, replies success, and the reservation system records the reservation.
* **Out of sync** (agent's ground truth says the device is not actually available) -- the
  agent refuses the reservation **and** pushes an authoritative state update back into the
  registry to correct the approximation. The reservation system then picks a different candidate
  and retries. No human intervention is required for the registry to converge back to truth.

Once a reservation succeeds, the same agent that owns the device is responsible for
launching test execution against it  and dispatching it to the reserved device. When test execution
completes, the agent publishes the result *and* updates the registry to mark
the device available again.

The diagram below shows the full flow, with the two failure modes drawn explicitly.  Only
Host A's interior is expanded to show the decision point and self-healing flow:

.. graphviz::
   :alt: A reservation system uses an approximate registry to pick a device, then asks the owning host's nano_services agent to reserve it. Inside Host A, an explicit decision point compares the agent's ground truth against the registry approximation: the IN-SYNC branch reserves the device locally and launches the test before releasing it on completion; the OUT-OF-SYNC branch refuses the reservation and pushes an authoritative state update back into the registry to self-heal it. Host B follows the same pattern for its own devices.
   :align: center
   :caption: Distributed device reservation across multiple hosts
   :layout: neato

   digraph DeviceReservationFlow {
       splines=spline;
       overlap=false;
       sep="+8";
       bgcolor="white";
       node [fontname="Helvetica", fontsize=12];
       edge [fontname="Helvetica", fontsize=11];

       subgraph cluster_central {
           label="Central Services";
           labelloc="t";
           fontname="Helvetica-Bold";
           fontsize=12;
           style="rounded,filled";
           color="#3b6ea5";
           fillcolor="#eaf2fb";

           reservation [label="Reservation System\n(picks candidate, calls reserve())", shape=box,
                        style="rounded,filled", fillcolor="#ffffff", color="#3b6ea5",
                        pos="0,2!"];

           registry [label="<f0> Device Registry\n(APPROXIMATE state) | <d1> dev_a : avail? | <d2> dev_b : avail? | <d3> dev_c : avail? | <d4> ... | <dn> dev_N : avail?",
                     shape=record, style="filled", fillcolor="#fff8dc", color="#b58900",
                     pos="0,0.5!"];

           reservation -> registry:f0 [label="1. read approximate state"];
       }

       subgraph cluster_host_a {
           label="Host A  -- decision shown";
           labelloc="t";
           fontname="Helvetica-Bold";
           fontsize=12;
           style="rounded,filled";
           color="#2e8b57";
           fillcolor="#eaf7ee";

           agent_a [label="nano_services Agent A\n@web_api reserve(device_id)\n@web_api release(device_id)\nSOURCE OF TRUTH", shape=box,
                    style="rounded,filled", fillcolor="#ffffff", color="#2e8b57",
                    pos="5,3!"];

           decide_a [label="gnd truth\n agrees?",
                     shape=diamond, style="filled", fillcolor="#fff8dc", color="#b58900",
                     pos="7.5,3!"];

           reserve_ok_a [label="reserve device\nlocally", shape=box,
                         style="rounded,filled", fillcolor="#ffffff", color="#2e8b57",
                         pos="9.5,3.7!"];

           refuse_a [label="refuse reservation", shape=box,
                     style="rounded,filled", fillcolor="#ffffff", color="#a5533b",
                     pos="9.5,2.3!"];

           dev_c [label="dev_c", shape=box, style="rounded,filled", fillcolor="#ffffff", color="#2e8b57",
                  pos="11.5,3.95!"];
           dev_b [label="dev_b", shape=box, style="rounded,filled", fillcolor="#ffffff", color="#2e8b57",
                  pos="11.5,3.4!"];

           agent_a -> decide_a [label="3. check"];
           decide_a -> reserve_ok_a [label="IN SYNC\n(available)", color="#2e8b57", fontcolor="#2e8b57"];
           decide_a -> refuse_a [label="OUT OF SYNC\n(busy /\nunavailable)", color="#a5533b", fontcolor="#a5533b"];

           reserve_ok_a -> dev_c [label="4a. launch test", style=dashed, color="#2e8b57"];
           reserve_ok_a -> dev_b [style=dashed, color="#2e8b57"];
       }

       subgraph cluster_host_b {
           label="Host B -- decision elided";
           labelloc="t";
           fontname="Helvetica-Bold";
           fontsize=12;
           style="rounded,filled";
           color="#a5533b";
           fillcolor="#fbeeea";

           agent_b [label="Agent B\nreserve()\nrelease()\n(decision\nelided)", shape=box,
                    style="rounded,filled", fillcolor="#ffffff", color="#a5533b",
                    width=1.6, fixedsize=false,
                    pos="4,5.5!"];

           dev_a [label="dev_a", shape=box, style="rounded,filled", fillcolor="#ffffff", color="#a5533b",
                  width=0.9, fixedsize=false,
                  pos="9.5,5.5!"];

           agent_b -> dev_a [label="run test", style=dashed, color="#a5533b"];
       }

       reservation -> agent_a [label="2. reserve(dev_c)"];
       reservation -> agent_b [label="2'. reserve(dev_a)", color="#a5533b"];

       reserve_ok_a -> reservation [label="4a. OK", color="#2e8b57"];
       refuse_a -> reservation [label="4b. refused; retry", color="#a5533b", style=dashed];
       refuse_a -> registry:d3 [label="4b. push authoritative state\n(self-heal)", color="#a5533b", style=bold];

       dev_c -> agent_a [label="5a. tests complete", style=dotted, color="#2e8b57"];
       agent_a -> registry:d3 [label="6a. release(dev_c)\n(-> available)", color="#2e8b57", style=dotted];

       caption [shape=plaintext, fontsize=10,
                label="Each host runs its own @web_api agent and applies the same IN-SYNC / OUT-OF-SYNC decision; Host B's interior is collapsed to keep the diagram readable."];
       { rank=sink; caption; }
   }

How this maps to ``@web_api`` declarations:

* Each host runs a ``WebApplication`` exposing a small class -- for example
  ``DeviceAgent`` -- with ``@web_api`` methods such as
  ``reserve(device_id: str) -> ReservationResult``,
  ``release(device_id: str) -> None``, and
  ``current_state() -> dict[str, DeviceState]``. The reservation system invokes these as
  ordinary Python calls via the client proxy described in
  :ref:`A More Complete Example <a-more-complete-example>` -- the HTTP transport is hidden.
* The "push authoritative state" arrow is implemented by the agent calling the reservation
  system's own ``@web_api`` (e.g., ``Registry.update(host, device_id, state)``) at the
  moment it detects an out-of-sync request -- nano-service-to-nano-service traffic uses the
  same Python-shaped API.
* The "launch test" arrow is the agent pulling from the ``SourceQueueFeed`` from
  :doc:`distributed_queues` and dispatching the test case to the reserved device locally;
  the corresponding result is pushed to the ``SinkQueueConsumer`` on completion.

The remainder of this page walks through the API surface that makes such a deployment
possible.


.. _a-more-complete-example:

A More Complete Example
-----------------------

.. automodule:: hydra.nano_services.api
   :no-index:


.. _required-type-annotations-and-restrictions:

Required Type Annotations and Restrictions
------------------------------------------

.. automodule:: hydra.nano_services.conversions
   :no-index:


Auto-Generated JavaScript Client
--------------------------------

.. automodule:: hydra.nano_services.js
   :no-index:

Example: streaming device telemetry to a browser dashboard
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To make the value of the auto-generated JavaScript client concrete, consider a host running a
``DeviceMonitor`` ``@web_api`` that samples CPU and memory usage on a *device under test* and
yields periodic readings. With ``using_async=True`` and a streaming return type, the same
``@web_api`` becomes, on the browser side, an ``async`` generator the dashboard can iterate
to drive live bar / gauge charts -- no hand-written WebSocket, polling loop, or fetch
plumbing required.

.. code-block:: python

    # device_monitor.py -- SERVER (runs on the host that owns the DUT)
    from typing import AsyncIterator
    from dataclasses import dataclass

    from hydra.nano_services.http import web_api, RestMethod


    @dataclass
    class Sample:
        timestamp: float
        cpu_percent: float      # 0..100
        memory_percent: float   # 0..100


    class DeviceMonitor:

        @classmethod
        @web_api(content_type='application/json', method=RestMethod.GET)
        async def telemetry(cls, device_id: str,
                            interval_s: float = 1.0) -> AsyncIterator[Sample]:
            \"\"\"
            Stream periodic CPU / memory samples for the given device.

            :param device_id: id of the device under test
            :param interval_s: seconds between samples
            :return: a stream of Sample readings
            \"\"\"
            while True:
                yield read_sample(device_id)
                await asyncio.sleep(interval_s)

On the browser side, the auto-generated client exposes ``DeviceMonitor.telemetry`` as an
``async`` generator. The dashboard simply consumes it with ``for await`` and renders each
sample as it arrives:

.. code-block:: javascript

    // dashboard.js -- BROWSER (loaded from /static/js/<js_bundle_name>.js)
    import { DeviceMonitor } from "/static/js/my_api.js";

    async function runDashboard(deviceId) {
        for await (const sample of DeviceMonitor.telemetry(deviceId, 1.0)) {
            updateCpuBar(sample.cpu_percent);        // bar chart
            updateMemoryGauge(sample.memory_percent); // gauge chart
        }
    }

    runDashboard("dut-42");

The end-to-end flow looks like:

.. graphviz::
   :alt: A DeviceMonitor nano-service on the host samples CPU and memory usage on a device under test at a configurable interval and yields each Sample. hydra.nano_services streams each yielded sample as an HTTP streamed response. The auto-generated JavaScript client exposes the @web_api as an async generator the browser dashboard iterates with "for await", updating a live CPU bar chart and a memory gauge as samples arrive.
   :align: center
   :caption: Streaming CPU/memory telemetry from a device to a live browser dashboard (Javascript API).

   digraph DeviceTelemetryStream {
       rankdir=LR;
       splines=spline;
       nodesep=0.4;
       ranksep=0.7;
       bgcolor="white";
       node [fontname="Helvetica", fontsize=11];
       edge [fontname="Helvetica", fontsize=10];

       subgraph cluster_host {
           label="Host (owns Device Under Test)";
           labelloc="t";
           fontname="Helvetica-Bold";
           fontsize=12;
           style="rounded,filled";
           color="#2e8b57";
           fillcolor="#eaf7ee";

           dut [label="Device Under Test\n(dut-42)", shape=box3d,
                style="filled", fillcolor="#ffffff", color="#2e8b57"];

           monitor [label="DeviceMonitor (nano_services)\n@web_api telemetry(device_id, interval_s)\n-> AsyncIterator[Sample]", shape=box,
                    style="rounded,filled", fillcolor="#ffffff", color="#2e8b57"];

           dut -> monitor [label="sample(cpu, mem)\nevery interval_s", style=dashed, color="#2e8b57"];
       }

       subgraph cluster_browser {
           label="Browser (dashboard.js + auto-generated JS client)";
           labelloc="t";
           fontname="Helvetica-Bold";
           fontsize=12;
           style="rounded,filled";
           color="#3b6ea5";
           fillcolor="#eaf2fb";

           jsclient [label="DeviceMonitor.telemetry(...)\nauto-generated async* generator", shape=box,
                     style="rounded,filled", fillcolor="#ffffff", color="#3b6ea5"];

           cpu_bar [label="CPU usage  (bar chart)\n  0% |##########.........| 100%\n          ~ 52 %", shape=note,
                    style="filled", fillcolor="#fff8dc", color="#b58900"];

           mem_gauge [label="Memory  (gauge)\n        .--------.\n       /    73 %   \\\\\n      |  -----+-----  |\n       \\\\   /   |     /\n        '--/----'----'", shape=note,
                      style="filled", fillcolor="#fff8dc", color="#b58900"];

           jsclient -> cpu_bar [label="updateCpuBar(...)", color="#3b6ea5"];
           jsclient -> mem_gauge [label="updateMemoryGauge(...)", color="#3b6ea5"];
       }

       monitor -> jsclient [label="HTTP streamed response\ntext/streamed; charset=x-user-defined\n(one Sample per yield)", color="#b58900", style=bold];
   }

What the JavaScript generator did for free:

* Translated the ``AsyncIterator[Sample]`` return type into an ``async`` generator on the
  client, so the browser-side caller uses idiomatic ``for await ... of`` syntax.
* Translated the ``Sample`` dataclass into a plain JavaScript object with ``timestamp``,
  ``cpu_percent``, ``memory_percent`` fields (per the rules in
  :ref:`Required Type Annotations and Restrictions <required-type-annotations-and-restrictions>`).
* Routed the call to the correct HTTP method (``GET``) and URL
  (``/DeviceMonitor/telemetry``), encoded ``device_id`` and ``interval_s`` as query-string
  parameters, and selected the streaming content type automatically.
* Generated JSDoc on the JavaScript ``telemetry`` function from the Python docstring, so the
  dashboard developer's IDE autocompletes parameters with their types and documentation.

Switching the server to ``using_async=False`` would break this example: the synchronous
JavaScript flavor performs a single blocking ``XMLHttpRequest`` and returns one value, with
no mechanism to receive subsequent samples. Streaming telemetry **requires** the async
flavor (the default).

This example may be better suited for websockets in a real implementation, but it serves to illustrate how the auto-generated
JavaScript client turns a streaming ``@web_api`` into an idiomatic async generator on the browser side,
with zero hand-written client code.  A future planned feature is to support WebSocket transport via a
"@ws_api" decoratror.
