"""
The module *hydra.nano_services* contains a framework for running an HTTP server,
built on top of *aiohttp*, providing a convenience layer of abstraction.
Its primary use is the ability to set up quick nano-services that interact through a common
Python API, with the transaction details of HTTP hidden from the developer.
The framework allows auto-generation of corresponding JavaScript APIs to match the Python API
for use in a web browser.  The developer need not be concerned about the details of how to map
routes nor to understand the details of HTTP transactions.
The developer need only focus on development of a Python API that can automatically act as a
web API and as a remote procedure call (RPC) mechanism between distributed Python applications.

Getting Started
---------------

Let's look at setting up a simple WebApplication on your localhost:

.. code-block:: python

    # salutations.py
    import asyncio
    from pathlib import Path

    from hydra.nano_services.http import web_api, RestMethod, WebApplication


    class Greetings:

        @classmethod
        @web_api(content_type='text/html', method=RestMethod.GET)
        async def welcome(cls, name: str) -> str:
            \"\"\"
            Welcome someone

            :param name: name of person to greet
            :return: a salutation of welcome
            \"\"\"
            return f"<html><body><p>Welcome, {name}!</p></body></html>"

        @classmethod
        @web_api(content_type='text/html', method=RestMethod.GET)
        async def goodbye(cls, type_of_day: str) -> str:
            \"\"\"
            Tell someone goodbye by telling them to have a day (of the given type)

            :param type_of_day: an adjective describing what type of day to have
            :return: a salutation of farewell
            \"\"\"
            return f"<html><body><p>Have a {type_of_day} day!</p></body></html>"


    if __name__ == '__main__':
        app = WebApplication(static_path=Path.cwd())
        asyncio.run(app.start())  # defaults to localhost HTTP on port 8080

Saving this to a file, ``salutations.py``, you can run it to start your server:

.. code-block:: bash

    % python3 salutations.py

Then open a browser to the following URLs:

* http://localhost:8080/Greetings/welcome?name=Bob
* http://localhost:8080/Greetings/goodbye?type_of_day=wonderful

to display various salutations. The class ``Greetings`` can be called:

#. directly from Python (as though the decorators were not there),
#. from Python code running on a remote host (as a ReST API), and
#. from JavaScript running in a web browser (as an auto-generated JavaScript API).

Although the logic should function the same in each case, the timing of execution will vary.

Overall, the ``@web_api`` decorator provides the hooks for *hydra.nano_services* to do its job:
creating a known route map based on code structure, generating JavaScript when a ``static_path``
is provided, and handling the details of HTTP transactions. The developer can be blissfully
unaware of the HTTP protocol behind the Python APIs.


.. caution::

    Although the code prevents name collisions, the underlying (automated) routes do not, and a
    route must be unique. Thus, each pair of class/method declared as a ``@web_api`` must be
    unique, even across differing modules.

.. caution::

    *hydra.nano_services* invokes all requests in a single thread within the server (and undoes
    any per-thread model of the underlying http/web package used to conduct the HTTP
    transactions). The user needs to be keenly aware of the rules of asyncio. Specifically:
    (1) do not invoke blocking calls (``await`` calls that yield processing back but take a
    long time to complete are OK, of course) and (2) understand that if stateful, the state
    of the server objects can change in the middle of execution if an ``await`` is invoked
    and processing is yielded to another async request from another client call.

"""


class HTTPException(Exception):

    def __init__(self, code: int, msg: str):
        super().__init__(msg)
        self._code = code

    @property
    def status_code(self):
        return self._codesud
