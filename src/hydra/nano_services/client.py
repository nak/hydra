"""
hydra.nano_services provides and abstraction to easily declare Python clients to interact with a hydra.nano_services web application.

To access client code, a server implementation must inherit from an abstract interface common to both client
and server.  One defines an interface class (usually in its own file) such as:

>>>  from hydra.nano_services import WebInterface
...  from hydra.nano_services import RestMethod
...  from hydra.nano_services web_api
...  from typing import AsyncIterator
...  from abc import abstractmethod
...
...  class MyServerApiInterface(WebInterface):
...     @classmethod
...     @web_api(methos=RestMethod.GET, content_type='application/json')
...     @abstractmethod
...     async def constructor(cls) -> "MyServerApiInterface":
...        '''
...        Abstract constructor to create an instance of the class
...        '''
...
...      @classmethod
...      @web_api(method=RestMethod.GET, content_type='text/plain')
...      @abstractmethod
...      async def class_method_api(cls, parm1: str) -> Dict[str, int]:
...          '''
...           Abstract class method to be implemented WITH SAME @web_api DECORATION
...          '''
...
...      @web_api(method=RestMethod.POST, content_type='application/json')
...      @abstractmethod
...      async def instance_method_api(self) -> AsyncIterator[str]:
...          '''
...          Abstract instance method that is an Async Iterator over string values. MUST
...          HAVE SAME @web_api DECORATION AS SERVER IMPL DECLARATION
...          '''
...          # never called since abstract, but needed to tell Python that implementation
...          # will be an async generator/iterator:
...          yield None
...

One then defines the concrete class (which for best practice, has same name, sans 'Interface').  The
@web_api decorators need not be specified as hydra.nano_services will ensure they are inherited. (But if you do
specify them explicitly, you must ensure they are maintained to be the same):

>>>  from hydra.nano_services import WebInterface
...  from hydra.nano_services import RestMethod
...  from hydra.nano_services web_api
...  from typing import AsyncIterator
...
...  class MyServerApi(MyServerApiInterface):
...
...     def __init__(self):
...         self._message_bits = ['I', 'am', 'the', 'very', 'model']
...
...     @classmethod
...     async def constructor(cls) -> "MyServerApiInterface":
...        '''
...        Concreate constructor to create an instance of the class WITH SAME @web_api DECORATOR AND
...        SIGNATURE
...        '''
...        return MyServerApi()
...
...      @classmethod
...      async def class_method_api(cls, parm1: str) -> Dict[str, int]:
...          '''
...          Concreate class method to be implemented WITH SAME @web_api DECORATION AND,
...          OF COURSE, SIGNATURE
...          '''
...          return {'param1': int(parm1)}
...
...      async def instance_method_api(self) -> AsyncIterator[str]:
...          '''
...          Concrete instance method that is an Async Iterator over string values.
...          MUST HAVE SAME @web_api DECORATION AND SIGNATURE
...          '''
...          # never called since abstract, but needed to tell Python that implementation
...          # will be an async generator/iterator:
...          for text in self._message_bits:
...              yield text
...

One can then declare a Client that acts as a proxy to make calls to the server, thereby keeping the
http protocol details hidden (abstracted away frm the user):

>>>  async def async_call():
...      client_end_point_mapping = MyServerApiInterface.ClientEndpointMapping()
...      client = await client_end_point_mapping['https://blahblahblah']
...      instance = client.constructor()
...      async for text in instance.instance_method_api():
...          print(text)
...

If you do not fallow the recommended practice of the interface have the same name as the concreate class, only with
an "Interface" suffix, you will have to specify *impl_name=<name-of-concrete-class>* as a parameter to Client class
method above.  The *end_point* parameter specifies the base url to the server that serves up MyServceApi class.

"""
import asyncio
import inspect
import json
import sys
from abc import ABC
from functools import wraps
from typing import Any, Dict, TypeVar, Optional, Type, Generic, Mapping, Iterator

import aiohttp
from aiohttp import ClientConnectionError

from hydra.nano_services import conversions
from hydra.nano_services.api import API, RestMethod

__all__ = ["WebInterface"]

C = TypeVar('C', bound="WebInterface")


class InvocationError(Exception):
    def __init__(self, message: str):
        self.message = message

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.message


# noinspection PyProtectedMember
class WebInterface(ABC):

    _clients: Dict[str, Any] = {}

    @classmethod
    def _generate_url_args(cls, kwargs, self_id: Optional[str] = None):
        if self_id is None and not kwargs:
            return ''
        return (f'?self={self_id}&' if self_id is not None else '?') + \
            '&'.join([f"{k}={conversions.to_str(v)}" for k, v in kwargs.items() if v is not None])

    @classmethod
    def _add_class_method(cls, clazz: Type, impl_name: str, end_point: str, method,
                          common_headers: dict):
        # class/static methods
        # noinspection PyProtectedMember
        # noinspection PyProtectedMember
        name = method.__name__
        api: API = method._hydra_nano_services_web_api
        base_url = f"{end_point}/{impl_name}/{name}"

        # noinspection PyDecorator,PyShadowingNames
        @classmethod
        @wraps(method)
        async def class_method(cls_, *args, **kwargs_):
            nonlocal api, end_point
            # noinspection PyBroadException
            try:
                arg_spec = inspect.getfullargspec(api._func)
            except Exception:
                arg_spec = inspect.getfullargspec(api._func.__func__)
            if len(arg_spec) - 1 < len(args):
                raise TypeError(f"Too many arguments supplied in call to {api.name}")
            if arg_spec.varargs is None:
                kwargs_.update({
                    arg_spec.args[n + 1]: arg for n, arg in enumerate(args)
                })
            else:
                kwargs_[arg_spec.varargs] = args
            # noinspection PyBroadException
            rest_method = api._func._hydra_nano_services_web_api.method
            data = None
            resp = None
            try:
                async with aiohttp.ClientSession(timeout=api.timeout, headers=common_headers) as session:
                    if rest_method.value == RestMethod.GET.value:
                        url_args = cls._generate_url_args(kwargs=kwargs_)
                        url = f"{base_url}{url_args}"
                        async with session.get(url) as resp:
                            data = (await resp.content.read()).decode('utf-8')
                            if not resp.ok:
                                sys.stderr.write(data + '\n')
                            resp.raise_for_status()
                            if api.is_constructor:
                                if hasattr(clazz, 'jsonrepr'):
                                    repr_ = clazz.jsonrepr(data)
                                    self_id = repr_[api.uuid_param or 'uuid']
                                else:
                                    repr_ = json.loads(data)
                                    self_id = repr_[api.uuid_param or 'uuid']
                                return cls_(self_id)
                            return conversions.from_str(data, api.return_type)
                    else:
                        payload = json.dumps({conversions.to_str(k): conversions.normalize_to_json_compat(v)
                                              for k, v in kwargs_.items()})
                        async with session.post(base_url, data=payload) as resp:
                            data = (await resp.content.read()).decode('utf-8')
                            if not resp.ok:
                                sys.stderr.write(data + '\n')
                            resp.raise_for_status()
                            if api.is_constructor:
                                self_id = json.loads(data)[api.uuid_param or 'uuid']
                                return cls_(self_id)
                            return conversions.from_str(data, api.return_type)
            except aiohttp.ClientResponseError as e:
                error_body = data if resp is not None else "<<no response text/traceback info>>"
                error_body += f"\n\nRequest to {api.name} failed: {e.message}"
                raise InvocationError(error_body)

        setattr(clazz, name, class_method)

    @classmethod
    def _add_class_method_streamed(cls, clazz: Type, impl_name: str, end_point: str, method,
                                   common_headers: dict):
        if not hasattr(method, '_hydra_nano_services_web_api'):
            raise SyntaxError(f"All methods of class WebClient most be decorated with '@web_api'")
        # noinspection PyProtectedMember
        if method._hydra_nano_services_web_api.has_streamed_request:
            raise SyntaxError(f"Streamed request for WebClient's are not supported at this time")
        # noinspection PyProtectedMember

        name = method.__name__
        api: API = method._hydra_nano_services_web_api
        base_url = f"{end_point}/{impl_name}/{name}"

        # noinspection PyDecorator,PyUnusedLocal
        @classmethod
        async def class_method_streamed(cls_, *args, **kwargs):
            nonlocal api, end_point, common_headers
            resp = None
            # noinspection PyBroadException
            try:
                arg_spec = inspect.getfullargspec(api._func)
            except Exception:
                arg_spec = inspect.getfullargspec(api._func.__func__)
            kwargs.update({
                arg_spec.args[n + 1]: arg for n, arg in enumerate(args)
            })
            rest_method = api._func._hydra_nano_services_web_api.method
            common_headers = (common_headers or {}).update({'Content-Type': 'text/streamed'})
            try:
                if rest_method.value == RestMethod.GET.value:
                    url_args = cls._generate_url_args(kwargs=kwargs)
                    url = f"{base_url}{url_args}"
                    async with aiohttp.ClientSession(timeout=api.timeout, headers=common_headers) as session:
                        async with session.get(url) as resp:
                            if not resp.ok:
                                sys.stderr.write((await resp.content.read()).decode('utf-8') + '\n')
                            resp.raise_for_status()
                            buffer = ""
                            async for data, _ in resp.content.iter_chunks():
                                if data:
                                    if api.return_type != bytes:
                                        buffer += data.decode('utf-8')
                                        *data_items, buffer = buffer.split('\0')
                                        for datum in data_items:
                                            yield conversions.from_str(datum, api.return_type)
                                    else:
                                        data = data.decode('utf-8')
                                        yield conversions.from_str(data, api.return_type)
                else:
                    payload = json.dumps({conversions.to_str(k): conversions.normalize_to_json_compat(v)
                                          for k, v in kwargs.items()})
                    async with aiohttp.ClientSession(timeout=api.timeout, headers=common_headers) as session:
                        async with session.post(base_url, data=payload) as resp:
                            if not resp.ok:
                                sys.stderr.write((await resp.content.read()).decode('utf-8') + '\n')
                            resp.raise_for_status()
                            buffer = ""
                            async for data, _ in resp.content.iter_chunks():
                                if data:
                                    if api.return_type != bytes:
                                        buffer += data.decode('utf-8')
                                        *data_items, buffer = buffer.split('\0')
                                        for datum in data_items:
                                            yield conversions.from_str(datum, api.return_type)
                                    else:
                                        data = data.decode('utf-8')
                                        yield conversions.from_str(data, api.return_type)
            except aiohttp.ClientResponseError as e:
                body = e.message
                raise Exception(body)

        setattr(clazz, name, class_method_streamed)

    @classmethod
    def _add_instance_method(cls, clazz: Type, impl_name: str, end_point: str, method,
                             common_headers: dict):
        # class/static methods
        # noinspection PyProtectedMember
        # noinspection PyProtectedMember
        name = method.__name__
        api: API = method._hydra_nano_services_web_api
        base_url = f"{end_point}/{impl_name}/{name}"

        # noinspection PyDecorator,PyShadowingNames
        @wraps(method)
        async def instance_method(self, *args, **kwargs_):
            nonlocal api, end_point
            error_body = ""
            # noinspection PyBroadException
            try:
                arg_spec = inspect.getfullargspec(api._func)
            except Exception:
                arg_spec = inspect.getfullargspec(api._func.__func__)
            kwargs_.update({
                arg_spec.args[n]: arg for n, arg in enumerate(args)
            })
            # noinspection PyBroadException
            rest_method = api._func._hydra_nano_services_web_api.method
            try:
                if rest_method.value == RestMethod.GET.value:
                    url_args = cls._generate_url_args(self_id=self.self_id, kwargs=kwargs_)
                    url = f"{base_url}{url_args}"
                    async with aiohttp.ClientSession(timeout=api.timeout, headers=common_headers) as session:
                        async with session.get(url) as resp:
                            if not resp.ok:
                                error_body = await resp.content.read()
                            resp.raise_for_status()
                            data = (await resp.content.read()).decode('utf-8')
                            return conversions.from_str(data, api.return_type)
                else:
                    kwargs_['self'] = self.self_id
                    payload = json.dumps({conversions.to_str(k): conversions.normalize_to_json_compat(v)
                                          for k, v in kwargs_.items()})
                    async with aiohttp.ClientSession(timeout=api.timeout, headers=common_headers) as session:
                        async with session.post(base_url, data=payload) as resp:
                            if not resp.ok:
                                sys.stderr.write((await resp.content.read()).decode('utf-8') + '\n')
                            resp.raise_for_status()
                            data = (await resp.content.read()).decode('utf-8')
                            if api.is_constructor:
                                self_id = json.loads(data)['self_id']
                                return clazz(self_id)
                            return conversions.from_str(data, api.return_type)
            except aiohttp.ClientResponseError as e:
                try:
                    body = error_body or  "<<no response text/traceback info>>"
                except (ConnectionError, ClientConnectionError):
                    body = "<<no response text/traceback info>>"
                body += f"\n\nRequest to {api.name} failed: {e.message}"
                raise Exception(body)

        setattr(clazz, name, instance_method)

    @classmethod
    def _add_instance_method_streamed(cls, clazz: Type, impl_name: str, end_point: str, method,
                                      common_headers: dict):
        # class/static methods
        # noinspection PyProtectedMember
        # noinspection PyProtectedMember
        name = method.__name__
        api: API = method._hydra_nano_services_web_api
        base_url = f"{end_point}/{impl_name}/{name}"

        async def instance_method_streamed(self, *args, **kwargs_):
            nonlocal api, end_point, base_url
            arg_spec = inspect.getfullargspec(api._func)
            kwargs_.update({
                arg_spec.args[n + 1]: arg for n, arg in enumerate(args)
            })
            rest_method = api.method
            try:
                if rest_method == RestMethod.GET:
                    url_args = cls._generate_url_args(self_id=self.self_id, kwargs=kwargs_)
                    url = f"{base_url}{url_args}"
                    async with aiohttp.ClientSession(timeout=api.timeout, headers=common_headers) as session:
                        async with session.get(url) as resp:
                            if not resp.ok:
                                sys.stderr.write((await resp.content.read()).decode('utf-8') + '\n')
                            resp.raise_for_status()
                            buffer = ""
                            async for data, _ in resp.content.iter_chunks():
                                if data:
                                    if api.return_type != bytes:
                                        buffer += data.decode('utf-8')
                                        *data_items, buffer = buffer.split('\0')
                                        for datum in data_items:
                                            yield conversions.from_str(datum, api.return_type)
                                    else:
                                        data = data.decode('utf-8')
                                        yield conversions.from_str(data, api.return_type)
                else:
                    url = f"{base_url}?self={self.self_id}"
                    kwargs_['self'] = self.self_id
                    payload = json.dumps({k: conversions.to_str(v) for k, v in kwargs_.items()})
                    async with aiohttp.ClientSession(timeout=api.timeout, headers=common_headers) as session:
                        async with session.post(url, data=payload) as resp:
                            if not resp.ok:
                                sys.stderr.write((await resp.content.read()).decode('utf-8') + '\n')
                            resp.raise_for_status()
                            buffer = ""
                            async for data, _ in resp.content.iter_chunks():
                                if data:
                                    if api.return_type != bytes:
                                        buffer += data.decode('utf-8')
                                        *data_items, buffer = buffer.split('\0')
                                        for datum in data_items:
                                            yield conversions.from_str(datum, api.return_type)
                                    else:
                                        data = data.decode('utf-8')
                                        yield conversions.from_str(data, api.return_type)
            except aiohttp.ClientResponseError as e:
                body = resp.content if resp is not None else "<<no response text/traceback info>>"
                body += f"\n\nRequest to {api.name} failed: {e.message}"
                raise Exception(body)

        setattr(clazz, name, instance_method_streamed)

    # noinspection PyPep8Naming
    @classmethod
    def ClientEndpointMapping(cls: C, impl_name: Optional[str] = None,
                              common_headers: Optional[dict] = None) -> Mapping[str, C]:
        if cls == WebInterface:
            raise Exception("Must call Client with concrete class of WebInterface, not WebInterface itself")
        if impl_name is None:
            if not cls.__name__.endswith('Interface'):
                raise Exception("Call to Client must specify impl_name explicitly since class name does not end in "
                                "'Interface'")
            impl_name = cls.__name__[:-len('Interface')]

        class ClientFactory(Mapping[str, C]):

            def __iter__(self) -> Iterator[str]:
                for key in [k for k in WebInterface._clients if k.startswith(cls.__name__)]:
                    yield key

            def __len__(self) -> int:
                return len([k for k in WebInterface._clients if k.startswith(cls.__name__)])

            @staticmethod
            def add_dynamic_methods(clazz: Type, end_point: str):

                non_class_methods = inspect.getmembers(cls, predicate=inspect.isfunction)
                for name, method in non_class_methods:
                    if not hasattr(method, '_hydra_nano_services_web_api'):
                        continue
                    if isinstance(inspect.getattr_static(cls, name), staticmethod):
                        if hasattr(method, '_hydra_nano_services_web_api'):
                            raise Exception(
                                f"Static method {name} of {cls.__name__} cannot have @web_api decorator. "
                                "WebInterface's can only have instance and class methods that are @web_api's")
                        continue
                    if not inspect.iscoroutinefunction(method) and not inspect.isasyncgenfunction(method):
                        raise Exception(f"Function {name} of {cls.__name__} is not async as expected.")
                    if isinstance(inspect.getattr_static(cls, name), staticmethod):
                        raise Exception(f"Method {name} of {cls.__name__}")
                    else:
                        if inspect.isasyncgenfunction(method):
                            cls._add_instance_method_streamed(clazz, impl_name, end_point, method, common_headers)
                        else:
                            cls._add_instance_method(clazz, impl_name, end_point, method, common_headers)

                class_methods = inspect.getmembers(cls, predicate=inspect.ismethod)
                for name, method in class_methods:
                    if not hasattr(method, '_hydra_nano_services_web_api'):
                        continue
                    if name == cls.ClientEndpointMapping.__name__ or name.startswith('_'):
                        continue
                    if not inspect.iscoroutinefunction(method) and not inspect.isasyncgenfunction(method):
                        raise Exception(f"Function {name} of {cls.__name__} is not async as expected.")
                    if inspect.isasyncgenfunction(method):
                        cls._add_class_method_streamed(clazz, impl_name, end_point, method, common_headers)
                    else:
                        cls._add_class_method(clazz, impl_name, end_point, method, common_headers)

            def __getitem__(self, end_point: str):
                class Impl:

                    def __init__(self, self_id: str):
                        super().__init__()
                        self._id = self_id

                    @property
                    def self_id(self):
                        return self._id

                class ImplProper(Impl, Generic[C]):
                    """
                    provides proper typing on return value, without conflicting with Python's abstract class rules
                    which ignore dynamically added methods as above for the concrete implementations
                    """

                    def __init__(self, *args, **kwargs):
                        Impl.__init__(self, *args, **kwargs)

                while end_point.endswith('/'):
                    end_point = end_point[:-1]
                key = f"{cls.__name__}@{end_point}"
                if key in WebInterface._clients:
                    return WebInterface._clients[key]

                if key not in WebInterface._clients:
                    ClientFactory.add_dynamic_methods(Impl, end_point)
                    WebInterface._clients[key] = ImplProper
                return WebInterface._clients[key]

        return ClientFactory()
