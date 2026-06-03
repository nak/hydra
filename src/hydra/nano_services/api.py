# Copyright (c) 2026. This code is part of hydra.nano_services and is released under the LPGL license (see LICENSE).
"""
A more complete example of using *hydra.nano_services* separates the code for client and server
cleanly. *hydra.nano_services* has APIs to instantiate a client from an API interface as shown
below. The client and server code should live in their own modules, and the client should
depend only on an abstract interface of the server:

.. code-block:: python

    # greetings_interface.py -- INTERFACE DEFINITION
    from abc import abstractmethod

    from hydra.nano_services.http import web_api, RestMethod
    from hydra.nano_services.api import raise_is_abstract


    class GreetingsInterface:

        @classmethod
        @abstractmethod
        @web_api(content_type='text/html', method=RestMethod.GET)
        async def welcome(cls, name: str) -> str:
            \"\"\"
            Welcome someone

            :param name: name of person to greet
            :return: a salutation of welcome
            \"\"\"
            return raise_is_abstract()

        @classmethod
        @abstractmethod
        @web_api(content_type='text/html', method=RestMethod.GET)
        async def goodbye(cls, type_of_day: str) -> str:
            \"\"\"
            Tell someone goodbye by telling them to have a day (of the given type)

            :param type_of_day: an adjective describing what type of day to have
            :return: a salutation of farewell
            \"\"\"
            return raise_is_abstract()

The server implementation looks like:

.. code-block:: python

    # greetings.py -- SERVER IMPLEMENTATION
    from hydra.nano_services.http import web_api, RestMethod

    from greetings_interface import GreetingsInterface


    class Greetings(GreetingsInterface):

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

.. code-block:: python

    # main.py -- ENTRY POINT TO START SERVER
    import asyncio
    from pathlib import Path

    from hydra.nano_services.http import WebApplication


    if __name__ == '__main__':
        app = WebApplication(static_path=Path.cwd())
        # default to localhost HTTP on port 8080
        asyncio.run(app.start(modules=['greetings']))

The ``modules`` parameter to the ``WebApplication.start`` method specifies which modules to scan
for ``@web_api`` definitions.

Finally, the client code to instantiate a remote proxy to the server API looks like:

.. code-block:: python

    # client.py -- CLIENT CODE
    import asyncio

    from greetings_interface import GreetingsInterface


    GreetingsProxy = GreetingsInterface.ClientEndpointMapping()['http://localhost:8080/']


    async def main():
        response = await GreetingsProxy.welcome("Bob")
        print(response)


    if __name__ == '__main__':
        asyncio.run(main())

Once the server is running, running this client code will invoke the server-side logic to
retrieve and print a salutation to Bob.  Note that the client depends only on the abstract
interface definition, not on any concrete implementation of the server. This allows for clean
separation of client and server code, and also allows a single interface module to be reused
by multiple client implementations and across multiple deployments of the server.
"""
import inspect
import typing
from enum import Enum
from typing import Callable, Awaitable, AsyncGenerator, Type, TypeVar

from aiohttp import ClientTimeout

AsyncChunkIterator = Callable[[int], Awaitable[AsyncGenerator[bytes, None]]]
AsyncLineIterator = AsyncGenerator[str, None]


class RestMethod(Enum):
    GET = 'GET'
    POST = 'POST'


class API:

    def __init__(self, clazz, func, method: RestMethod, content_type: str, is_instance_method: bool,
                 is_class_method: bool,
                 is_constructor: bool,
                 on_disconnect: Callable[[], None] | None = None,
                 timeout: ClientTimeout | None = None,
                 expire_on_exit: bool = False, uuid_param: str | None = None):
        annotations = func.__annotations__
        self._clazz = clazz
        self._on_disconnect = on_disconnect
        self._is_class_method = is_class_method
        self._is_instance_method = is_instance_method
        self._is_static = not self._is_class_method and not self._is_instance_method
        self._expire_obj = expire_on_exit
        self._func = func
        self._real_func = func
        self._method = method
        self._timeout = timeout or ClientTimeout()
        self._vararg = None
        self._varkwds = None
        if 'return' not in annotations:
            raise TypeError(f"No annotation for return type in {func}")
        self._arg_annotations = {name: typ for name, typ in annotations.items() if name != 'return'}
        if not func.__name__.startswith('_'):
            sig = inspect.signature(func)
            for name in self._arg_annotations:
                if sig.parameters[name].kind == inspect.Parameter.VAR_POSITIONAL:
                    self._arg_annotations[name] = typing.Tuple[int, Ellipsis]
                    self._vararg = name
                elif sig.parameters[name].kind == inspect.Parameter.VAR_KEYWORD:
                    self._arg_annotations[name] = dict[str, self._arg_annotations[name]]
                    self._varkwds = name
        self._async_arg_annotations = {
            name: typ for name, typ in self._arg_annotations.items()
            if typ in (bytes, AsyncChunkIterator, AsyncLineIterator)
        }
        if len(self._async_arg_annotations) > 1:
            raise TypeError("At most one parameter can be and async iterator in a web_api request")
        self._return_type = annotations['return']
        self._has_streamed_response = False
        if str(self._return_type).startswith('typing.AsyncIterator') or self._return_type == typing.AsyncIterator:
            self._return_type = self._return_type.__args__[0]
            self._has_streamed_response = True
        elif str(self._return_type).startswith('typing.AsyncGenerator') or self._return_type == typing.AsyncGenerator:
            self._return_type = self._return_type.__args__[0]
            self._has_streamed_response = True
        self._content_type = content_type if not self.has_streamed_response else 'text/streamed; charset=x-user-defined'
        self._is_constructor = is_constructor
        if is_constructor:
            self._return_type = str
        self._uuid_param = uuid_param

    @property
    def clazz(self):
        return self._clazz

    @property
    def timeout(self) -> ClientTimeout:
        return self._timeout

    @property
    def name(self) -> str:
        return self._real_func.__name__

    @property
    def qualname(self) -> str:
        return self._real_func.__qualname__

    @property
    def expire_object(self) -> bool:
        return self._expire_obj

    @property
    def is_class_method(self):
        return self._is_class_method

    @property
    def is_static(self):
        return self._is_static

    @property
    def is_constructor(self) -> bool:
        return self._is_constructor

    @property
    def uuid_param(self) -> str | None:
        return self._uuid_param

    @property
    def module(self) -> str:
        return self._real_func.__module__

    @property
    def doc(self) -> str:
        return self._real_func.__doc__

    @property
    def method(self):
        return self._method

    @property
    def on_disconnect(self):
        return self._on_disconnect

    @property
    def is_instance_method(self) -> bool:
        return self._is_instance_method

    @property
    def content_type(self) -> str:
        return self._content_type

    @property
    def arg_annotations(self):
        return self._arg_annotations

    @property
    def async_arg_annotations(self) -> dict[str, Type]:
        return self._async_arg_annotations

    @property
    def synchronous_arg_annotations(self) -> dict[str, Type]:
        return {a: v for a, v in self._arg_annotations.items() if a not in self._async_arg_annotations}

    @property
    def return_type(self):
        return self._return_type

    @property
    def has_streamed_request(self) -> bool:
        return len(self._async_arg_annotations) > 0

    @property
    def has_streamed_response(self) -> bool:
        return self._has_streamed_response

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def update_varargs(self, kwargs: dict[str, typing.Any]) -> [dict[str, typing.Any], typing.Tuple[typing.Any]]:
        """
        takes list of kwargs for the call and transforms as needed for any varargs  or var-kwargs in call
        """
        sig = inspect.signature(self._func)
        updated = kwargs
        varargs = tuple()
        for name, param in sig.parameters.items():
            if name not in kwargs:
                continue
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                del updated[name]
                updated.update(**kwargs[name])
            elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                varargs = kwargs[name]
                del updated[name]
        return updated, varargs


class APIDoc:
    """
    Class for converting python docs to target language/protocol (javascript, ReST API, ...)
    """

    class Flavor(Enum):
        JAVASCRIPT = "javascript"
        REST = "rest"
        PYTHON = "python"

    # noinspection PyProtectedMember
    @classmethod
    def generate(cls, flavor: Flavor, api: API, indent: str = "") -> str:
        """
        Add documentation for given method.

        :param flavor: which language/protocol to document
        :param api: which api to document
        :param indent: how much to indent each doc line by
        """
        if flavor == cls.Flavor.PYTHON:
            return '"""\n' + api._func.__doc__ + '\n"""\n'
        main_doc = ""
        type_names: dict[str, str] = {}
        if flavor == cls.Flavor.REST:
            route = '/' + api._func.__qualname__.replace('.', '/')
            top_line = f"ROUTE: {api.method.value} {route}"
            main_doc += f"\n{top_line}\n"
            main_doc += '~'*len(top_line) + '\n'
            main_doc += f"\nContent-Type: {api.content_type}\n"
        elif flavor == cls.Flavor.JAVASCRIPT:
            main_doc += f"{indent}/**"
        return_type_name = cls._get_type_name(api.return_type) if api.return_type else None
        for line in (api._func.__doc__ or "<<no documentation provided>>").splitlines():
            line = line.strip()
            if line.startswith('...') or line.startswith('>>>'):
                continue
            if line.startswith(':param '):
                name, doc = line[6:].strip().split(':', maxsplit=1)
                if name not in api.arg_annotations:
                    raise LookupError(f"Paremeter with name {name} found in doc string but no such parameter"
                                      f"exists in {api._func}")
                typ = api.arg_annotations[name]
                type_names[name] = cls._get_type_name(typ)
                main_doc += indent + f"**param**: $${name}$$ -- {doc}\n"
            elif line.startswith(':return'):
                _, doc = line[7:].strip().split(':', maxsplit=1)
                if api.has_streamed_response:
                    doc = "[$$streamed$$] " + doc
                main_doc += '\n' + indent + f"$$return$$ {doc}\n"
            elif line.startswith(':raises'):
                _, doc = line[7:].strip().split(':', maxsplit=1)
                main_doc += indent + f"$$raises$$ {doc}"
            else:
                main_doc += indent + line + '\n'
        for name in [arg for arg in api.arg_annotations if arg not in type_names]:
            main_doc += f"\n{indent}*undocumented param*: {name} of type" \
                        f" {cls._get_type_name(api.arg_annotations[name])}"
            type_names[name] = api.arg_annotations[name]
        main_doc += "\n"
        if flavor == cls.Flavor.JAVASCRIPT:
            for name in api.arg_annotations:
                main_doc = main_doc.replace(f"$${name}$$", f"{name} {{{type_names[name]}}}")
            main_doc = main_doc.replace("$$return$$", f"@return {{{return_type_name}}}")
            main_doc = main_doc.replace('**param**', '@param')
            main_doc = main_doc.replace('**return**', '@return')
            main_doc = main_doc.replace('$$raises$$', 'raises exception:')
            main_doc = main_doc.replace('$$streamed$$', 'iterator')
        elif flavor == cls.Flavor.REST:
            for name in api.arg_annotations:
                main_doc = main_doc.replace(f"$${name}$$", f"{name} *{type_names[name]}* -- ")
            from .http import WebApplication
            if api in WebApplication._instance_methods_class_map or api.name == '_expire':
                self_param = "**param**: self {{string}} -- unique id of a created instance"
                if '**param**' in main_doc:
                    main_doc = main_doc.replace('**param**', self_param + '\n**param**', 1)
                elif '**return**' in main_doc:
                    main_doc = main_doc.replace('**return**', self_param + '\n**return**', 1)
                else:
                    main_doc += self_param + '\n'

            main_doc = main_doc.replace('**return**', "**response** ")
            main_doc = main_doc.replace('**param**', "\n**param** ")
            main_doc = main_doc.replace('$$return$$', f"**response**: *{return_type_name}* -- ")
            main_doc = main_doc.replace('$$raises$$', "**error status**")
            main_doc = main_doc.replace('$$streamed$$', 'streamed')
        else:
            raise ValueError(f"Unknown doc type: {flavor}")
        if flavor == cls.Flavor.JAVASCRIPT:
            main_doc += f"{indent}**/\n"
        else:
            main_doc += "\n"
        return main_doc

    @staticmethod
    def _get_type_name(typ) -> str:
        """
        :return: string name for given type
        """
        if str(typ).startswith('typing.AsyncIterator') or str(typ).startswith('typing.AsyncGenerator'):
            root_type = typ.__args__[0]
            return f"iterator of type {root_type}"
        elif str(typ).startswith('typing.Union'):
            root_type = str(typ).split('[')[1].split(',')[0]
            return f"[optional] {root_type}"
        elif str(typ).startswith('typing.List'):
            root_type = str(typ).split('[')[1].replace(']', '')
            return f"list of {root_type}"
        elif str(typ).startswith('typing.Dict') or str(typ).startswith('typing.Mapping'):
            root_type = str(typ).split('[')[1].replace(']', '')
            return f"dict of {root_type}"
        elif str(typ).startswith('typing.Dict') or str(typ).startswith('typing.Mapping'):
            key_type, value_type = str(typ).split('[')[1].replace(']', '').split(',')
            return f"Dictionary of key type {key_type} and value type {value_type}"
        else:
            # noinspection PyProtectedMember
            return typ.__name__ if hasattr(typ, '__name__') else typ._name if hasattr(typ, '_name') else str(typ)


V = TypeVar('V')


def raise_is_abstract(v: V = None) -> V:
    raise NotImplementedError("Call to abstract function")
