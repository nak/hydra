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

The system is described by two diagrams. The first shows the **nominal end-to-end flow**
across two hosts and three devices, with the agents' internal decision points elided. The
second zooms in on a **single agent's decision point** -- the in-sync / out-of-sync branch
inside an agent -- to show how the registry self-heals.

**Nominal flow (overview).** A reservation is picked from the registry, sent to the owning
host's agent, accepted, the test runs against the reserved device, and on completion the
device is released back to the registry. Both hosts and all three devices are shown; the
decision point inside each agent is collapsed:

.. graphviz::
   :alt: A reservation system reads the approximate registry, picks a device, and asks the owning host's nano_services agent to reserve it. Two hosts are shown, with three devices distributed between them. The agents accept the reservation and run the test on the reserved device, then release it back to the registry on completion. The internal decision point in each agent is collapsed.
   :align: center
   :caption: Nominal reservation flow across two hosts and three devices; agent decision points are elided.
   :layout: neato

   digraph DeviceReservationOverview {
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
           label="Host A  -- decision elided";
           labelloc="t";
           fontname="Helvetica-Bold";
           fontsize=12;
           style="rounded,filled";
           color="#2e8b57";
           fillcolor="#eaf7ee";

           agent_a [label="Agent A\nreserve()\nrelease()\n(decision\nelided)", shape=box,
                    style="rounded,filled", fillcolor="#ffffff", color="#2e8b57",
                    width=1.6, fixedsize=false,
                    pos="5,3!"];

           dev_b [label="dev_b", shape=box, style="rounded,filled", fillcolor="#ffffff", color="#2e8b57",
                  width=0.9, fixedsize=false,
                  pos="9.5,3.4!"];
           dev_c [label="dev_c", shape=box, style="rounded,filled", fillcolor="#ffffff", color="#2e8b57",
                  width=0.9, fixedsize=false,
                  pos="9.5,2.6!"];

           agent_a -> dev_b [label="run test", style=dashed, color="#2e8b57"];
           agent_a -> dev_c [style=dashed, color="#2e8b57"];
       }

       subgraph cluster_host_b {
           label="Host B  -- decision elided";
           labelloc="t";
           fontname="Helvetica-Bold";
           fontsize=12;
           style="rounded,filled";
           color="#a5533b";
           fillcolor="#fbeeea";

           agent_b [label="Agent B\nreserve()\nrelease()\n(decision\nelided)", shape=box,
                    style="rounded,filled", fillcolor="#ffffff", color="#a5533b",
                    width=1.6, fixedsize=false,
                    pos="5,5.5!"];

           dev_a [label="dev_a", shape=box, style="rounded,filled", fillcolor="#ffffff", color="#a5533b",
                  width=0.9, fixedsize=false,
                  pos="9.5,5.5!"];

           agent_b -> dev_a [label="run test", style=dashed, color="#a5533b"];
       }

       reservation -> agent_a [label="2. reserve(dev_b)", color="#2e8b57"];
       reservation -> agent_b [label="2'. reserve(dev_a)", color="#a5533b"];

       agent_a -> reservation [label="3. OK", color="#2e8b57"];
       agent_b -> reservation [label="3'. OK", color="#a5533b"];
       dev_b -> agent_a [label="4. tests complete", style=dotted, color="#2e8b57"];
       agent_a -> registry:d2 [label="5. release(dev_b)\n(-> available)", color="#2e8b57", style=dotted];
       dev_a -> agent_b [label="4'. tests complete", style=dotted, color="#a5533b"];
       agent_b -> registry:d1 [label="5'. release(dev_a)\n(-> available)", color="#a5533b", style=dotted];
   }

**Decision point detail.** When the registry's approximation diverges from an agent's ground
truth, the reservation must be refused *and* the registry corrected. The diagram below
zooms into a single agent (here owning two devices) and shows the two outcomes of the
decision -- only the two outcome boxes, no other clusters:

.. graphviz::
   :alt: When the registry and host (ground truth) device status are out of sync
   :align: center
   :caption: When the registry and host (ground truth) device status are out of sync
   :layout: neato

   digraph DeviceReservationDecision {
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
                        pos="0,-0.5!"];

           registry [label="{ <f0> Device Registry\n(APPROXIMATE state) | <dx> dev_x : avail? | <dy> dev_y : avail? | <d4> ... | <dn> dev_N : avail? }",
                     shape=record, style="filled", fillcolor="#fff8dc", color="#b58900",
                     pos="5,-0.5!"];
       }

       subgraph cluster_host {
           label="Host C";
           labelloc="t";
           fontname="Helvetica-Bold";
           fontsize=12;
           style="rounded,filled";
           color="#2e8b57";
           fillcolor="#eaf7ee";

           agent [label="Agent\n@web_api\ \ \ \ \ \ \ \ \ \ \ \ \ \n reserve(device_id)\n\nSOURCE OF TRUTH", shape=box,
                  style="rounded,filled", fillcolor="#ffffff", color="#2e8b57",
                  pos="0,2!"];

           decide [label="gnd truth\nagrees?",
                   shape=diamond, style="filled", fillcolor="#fff8dc", color="#b58900",
                   pos="2.5,2!"];

           reserve_ok [label="reserve device\nlocally", shape=box,
                       style="rounded,filled", fillcolor="#ffffff", color="#2e8b57",
                       pos="5,2.8!"];

           refuse [label="refuse + push\nauthoritative state\n(self-heal)", shape=box,
                   style="rounded,filled", fillcolor="#ffffff", color="#a5533b",
                   pos="5,1.2!"];

           dev_x [label="dev_x", shape=box, style="rounded,filled", fillcolor="#ffffff", color="#2e8b57",
                  width=0.9, fixedsize=false,
                  pos="7.5,3.2!"];
           dev_y [label="dev_y", shape=box, style="rounded,filled", fillcolor="#ffffff", color="#2e8b57",
                  width=0.9, fixedsize=false,
                  pos="7.5,2.4!"];

           agent -> decide [label="check"];
           decide -> reserve_ok [label="IN SYNC", color="#2e8b57", fontcolor="#2e8b57"];
           decide -> refuse [label="OUT OF SYNC", color="#a5533b", fontcolor="#a5533b"];

           reserve_ok -> dev_x [label="launch test", style=dashed, color="#2e8b57"];
           reserve_ok -> dev_y [style=dashed, color="#2e8b57"];
       }

       reservation -> agent [label="reserve(device_id)"];
       refuse -> registry:dx [label="update authoritative state", color="#a5533b", style=bold];
       refuse -> reservation [label="refused; retry\nwith different candidate", color="#a5533b", style=dashed];
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

Server-Side Exceptions and How They Reach the Client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because *hydra.nano_services* is an RPC mechanism over HTTP, an exception raised inside a
``@web_api`` method on the server cannot literally be re-raised as the same Python object on
the client -- the exception has to cross a network boundary. The framework handles this
translation so that the *client call still raises* when the server's call failed, even though
the wire is HTTP.

What happens on the server
""""""""""""""""""""""""""

When a ``@web_api`` method raises, the request handler in :mod:`hydra.nano_services.http`
catches the exception and turns it into a non-2xx HTTP response:

* The HTTP status code is set to ``400`` (the framework's generic "request failed" code; a
  few specific paths use other 4xx codes -- e.g., ``PermissionError`` and bad query strings
  are mapped onto their own ``response_from_exception`` calls).
* The response ``reason`` field carries a short tag describing what happened, typically of
  the form ``"Exception in processing: <exception class name>(<message>)"`` or
  ``"General exception in request: <str(exception)>"``.
* The response body carries the longer detail -- the stringified exception plus, where
  available, additional traceback text -- as ``text/plain``.

For **streaming responses** (``AsyncIterator[T]`` / ``AsyncGenerator[T, None]``), an exception
raised *after* the response has already started streaming is signaled by changing the
in-flight ``StreamResponse`` status from 200 to 400 mid-stream (see ``set_status(400, ...)``
in the streaming code path), so the client can detect that the stream ended in error rather
than completing cleanly.

What happens on the client
""""""""""""""""""""""""""

On the client side, the auto-generated Python proxy in :mod:`hydra.nano_services.client`
calls ``resp.raise_for_status()`` on every response. If the response was non-2xx, aiohttp
raises a ``ClientResponseError`` carrying the status code and reason. The proxy catches this
and re-raises it as :class:`hydra.nano_services.client.InvocationError`, with a message
composed of:

#. the original HTTP response body (the stringified server-side exception / traceback), and
#. a trailer of the form ``"Request to <method_name> failed: <reason>"``.

So a client caller wraps the proxy call in ``try``/``except InvocationError``:

.. code-block:: python

    from hydra.nano_services.client import InvocationError

    GreetingsProxy = GreetingsInterface.ClientEndpointMapping()['http://localhost:8080/']

    async def main():
        try:
            response = await GreetingsProxy.welcome("Bob")
            print(response)
        except InvocationError as e:
            # str(e) contains the server's exception detail plus the reason trailer.
            print(f"Remote call failed:\n{e}")

In other words, the client does **not** receive the *same* Python exception class that was
raised on the server -- it receives a single ``InvocationError`` whose message preserves the
server-side exception class name and message (and, where the server logged it, a stack
trace). This keeps the contract simple (clients always catch one exception type) while still
giving the developer the diagnostic information they need.

Exceptions
""""""""""

* **One exception type, by design.** There is no automatic mapping of arbitrary server-side
  exception classes onto matching client-side classes. The exception message is, however,
  the same.  On the client side, only an ``InvocationError`` will be raised.
* **Mid-stream errors also raise InvocationError.** For streaming endpoints, a successful start
  followed by a failure mid-yield reaches the client as the streamed response ending with a
  400 status -- the client iterator again surfaces this on the client side
  as an ``InvocationError`` when it tries to pull the next item.
* **HTTP-layer failures are also InvocationError.** Connection failures, timeouts, and
  certificate problems are caught at the same layer and surfaced as
  ``InvocationError`` (with a message that names the underlying aiohttp failure). The
  client doesn't need a separate ``try/except`` for transport vs. application errors.  This
  may change in the future.


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
