"""
Main application definition (HTTP app)
"""
import asyncio
from asyncio import Task

import docutils.core
import importlib
import inspect
import json
import os
import socket
import sys
import traceback
import types
import uuid as uuid_pkg

import logging
from aiohttp import ClientTimeout, ClientConnectionError
from aiohttp import web
from aiohttp.web import (
    Application,
    Request,
    Response,
    StreamResponse,
)
from asyncio import CancelledError
from contextlib import suppress
from pathlib import Path
from ssl import SSLContext
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Union,
    Type,
)

from asynciomultiplexer import asynciomultiplexer

from . import HTTPException
from .conversions import from_str, to_str, normalize_from_json
from .decorators import (
    PreProcessor,
    PostProcessor,
    web_api,
    WebApi,
)
from .api import AsyncChunkIterator, AsyncLineIterator, RestMethod, API, APIDoc
from .js_async import JavascriptGeneratorAsync
from .js import JavascriptGenerator

_all__ = ['WebApplication', web_api, AsyncChunkIterator, AsyncLineIterator, 'AsyncApi', RestMethod]

log = logging.getLogger("hydra.nano_services")
if "hydra.nano_services_LOG_LEVEL" in os.environ:
    log.setLevel(os.environ.get("hydra.nano_services_LOG_LEVEL"))
AsyncApi = Callable[['WebApplication', Request], Awaitable[StreamResponse]]
PathLike = Union[Path, str]
MAX_CLIENTS = 1024 ** 2

# noinspection PyBroadException
try:
    # UGH!!! ths underlying low-level implementation swallows
    # exceptions we need to process, so monkey patch :-(
    # noinspection PyUnresolvedReferences,PyProtectedMember
    from asyncio.selector_events import _SelectorTransport
    # noinspection PyProtectedMember
    _orig_fatal_error = _SelectorTransport._fatal_error

    def _fatal_error_override(self, exc, msg):
        _orig_fatal_error(self, exc, msg)
        if isinstance(exc, ConnectionError):
            raise

    _SelectorTransport._fatal_error = _fatal_error_override
except Exception:
    log.warn("This platform may not support on_disconnect for simple requests")


def wrap(app, op):
    async def wrapper(request):
        return await op(app, request)
    return wrapper


async def response_from_exception(request, reason: str, code: int = 400):
    text = traceback.format_exc()
    resp = Response(status=code,
                    reason=reason,
                    text=text,
                    content_type="text/plain"
                    )
    await resp.prepare(request)
    return resp


ASYNC_POLLING_INTERVAL = float(os.environ.get('hydra.nano_services_ASYNC_POLL', 0.15))


# noinspection PyUnresolvedReferences
class WebApplication:
    """
    Main class for running a WebApplication server. See the documentation for *aiohttp* for information on the various
    parameters.  Additional parameters for this class are:

    :param static_path: root path to where static files will be kept, mapped to a route "/static", if provided
    :param js_bundle_name: the name of the javascript file, **without** extension, where the client-side API
       will be generated, if provided
    """
    _context: Dict[Any, Request] = {}
    _class_instance_methods: Dict[Type, List[API]] = {}
    _instance_methods_class_map: Dict[API, Type] = {}
    _instance_methods: List[API] = []
    _all_methods: List[API] = []

    class MainThread:
        """
        This is used to "undo" the threading in aiohttp.  It seems silly to combine threading and asyncio
        by handling each request in its own response.  It is somewhat understandable to attempt to
        de-conflict state perhaps(?), but overall is just silly IMHO.  This thread puts all invocations
        of routes into one thread handle a main asyncio loop.  User needs to beware of stateful-ness in that
        one asyncio task can change the state while another is in the middle of being awaited (but threading
        really doesn't solve this anyway).
        """
        MAX_SIMULTANEOUS_REQUESTS = 1000

        request_q = asynciomultiplexer.AsyncAdaptorQueue(MAX_SIMULTANEOUS_REQUESTS)

        @classmethod
        async def start(cls, coro: Awaitable, resp_q: asynciomultiplexer.AsyncAdaptorQueue):
            await cls.request_q.put((coro, resp_q))

        @classmethod
        async def main(cls, initializer: Optional[Callable[[], None]] = None):
            if initializer:
                initializer()

            async def task(coro: Awaitable, resp_q: asynciomultiplexer.AsyncAdaptorQueue):
                try:
                    resp = await coro
                except Exception as e:
                    resp = e
                await resp_q.put(resp)

            while True:
                request = await cls.request_q.get(polling_interval=ASYNC_POLLING_INTERVAL)
                asyncio.create_task(task(*request))

    class ObjectRepo:
        DEFAULT_OBJECT_EXPIRATION: int = 60*60*2   # in seconds = 1 hour

        instances: Dict[str, Any] = {}
        by_instance: Dict[Any, str] = {}
        expiry: Dict[str, Task] = {}

        @classmethod
        async def expire_obj(cls, obj_id: str, new_lease_time: int):
            if new_lease_time > 0:
                await asyncio.sleep(new_lease_time)
            if hasattr(cls.instances[obj_id], '__aexit__'):
                with suppress(Exception):
                    await cls.instances[obj_id].__aexit__(None, None, None)
            del cls.instances[obj_id]
            del cls.expiry[obj_id]

    class DuplicateRoute(Exception):
        """
        Raised if an attempt is made to register a web_api function under an existing route
        """
        pass

    routes_get: Dict[str, AsyncApi] = {}
    routes_post: Dict[str, AsyncApi] = {}
    callables_get: Dict[str, API] = {}
    callables_post: Dict[str, API] = {}
    module_mapping_get: Dict[str, Dict[str, API]] = {}
    module_mapping_post: Dict[str, Dict[str, API]] = {}

    def __init__(self,
                 *,
                 static_path: Optional[PathLike] = None,
                 js_bundle_name: Optional[str] = None,
                 handler_args: Optional[Mapping[str, Any]] = None,
                 client_max_size: int = MAX_CLIENTS,
                 using_async: bool = True,
                 debug: Any = ..., ) -> None:  # mypy doesn't support ellipsis
        self._main_task: Optional[asyncio.Task] = None
        if static_path is not None and not Path(static_path).exists():
            raise ValueError(f"Provided static path, {static_path} does not exist")
        self._static_path = static_path
        self._js_bundle_name = js_bundle_name
        self._using_async = using_async
        self._web_app = Application(handler_args=handler_args,
                                    client_max_size=client_max_size, debug=debug)
        if static_path:
            self._web_app.add_routes([web.static('/static', str(static_path))])
        self._started = False
        self._preprocessor: Optional[PreProcessor] = None
        self._postprocessor: Optional[PostProcessor] = None
        self._all_apis: List[API] = []

    def _generate_rest_docs(self):
        rst_out = self._static_path.joinpath('_developer_docs.rst')
        html_out = self._static_path.joinpath('_developer_docs.html')

        def document_class(clazz_, doc_out):
            docs = clazz_.__doc__ or """<<no documentation provided>>"""
            title = f"Resource: {clazz_.__name__}"
            doc_out.write(f"""
{title}
{'-'*len(title)}

{docs.strip()}
""")

        with open(rst_out, 'w') as out:
            out.write("REST API DOCUMENTATION\n")
            out.write("======================\n\n")

            out.write("\nReST Resources and Methods")
            out.write("\n==========================\n")
            resources_seen = set()
            for (route, api), method in sorted(
                    [(item, RestMethod.GET) for item in WebApplication.callables_get.items()] +
                    [(item, RestMethod.POST) for item in WebApplication.callables_post.items()]
            ):
                resource, _ = route[1:].split('/', maxsplit=1)
                if resource not in resources_seen:
                    resources_seen.add(resource)
                    for clazz in WebApplication._class_instance_methods:
                        if clazz.__name__ == resource:
                            document_class(clazz, out)
                out.write(APIDoc.generate(api=api, flavor=APIDoc.Flavor.REST))
                out.write("\n")
        if html_out.exists():
            os.remove(html_out)
        css = """

        h2{
            margin: 1em 0 .6em 0;
            font-weight: normal;
            color: black;
            font-family: 'Hammersmith One', sans-serif;
            text-shadow: 0 -3px 0 rgba(255,255,255,0.8);
            position: relative;
            font-size: 30px;
            line-height: 40px;
            background-color: lightgrey;
            font-size:150%
        }

        h3 {
            margin: 1em 0 0.5em 0;
            color: #343434;
            font-size: 22px;
            line-height: 40px;
            font-weight: normal;
            font-family: 'Orienta', sans-serif;
            letter-spacing: 1px;
            font-style: italic;font-size: 90%; background-color: #fed8b1;
        }
        """
        css_dir: Path = self._static_path.joinpath('css')
        if not css_dir.exists():
            css_dir.mkdir(parents=True)
        css_file = css_dir.joinpath('docs.css')
        with open(css_file, 'w') as css_out:
            css_out.write(css)
        docutils.core.publish_file(
            source_path=str(rst_out),
            destination_path=str(html_out),
            writer_name="html",
            settings_overrides={'stylesheet_path': ','.join(["html4css1.css", str(css_file)])}
        )

    def set_preprocessor(self, processor: PreProcessor):
        self._preprocessor = processor

    @property
    def preprocessor(self):
        return self._preprocessor

    def set_postprocessor(self, processor: PostProcessor):
        self._postprocessor = processor

    @property
    def postprocessor(self):
        return self._postprocessor

    @classmethod
    def register_route_get(cls, route: str, async_handler: AsyncApi, api: API, module: str) -> None:
        """
        Register the given handler as a GET call. This should be called from the @web_api decorator, and
        rarely if ever called directly

        :param route: route to register under
        :param async_handler: the raw handler for handling an incoming Request and returning a Response
        :param api: the high-level decorated web_api that will be invoked by the handler
        :param module: module that is holding the @web_api
        """
        if route in cls.routes_get or route in cls.routes_post:
            existing = cls.callables_get.get(route) or cls.callables_post.get(route)
            if existing and (api.module != existing.module or api.name != existing.name):
                raise cls.DuplicateRoute(
                    f"Route '{route}' associated with {api.module}.{api.name}"
                    f" already exists here: {existing.module}.{existing.name} "
                )
        cls.routes_get[route] = async_handler
        cls.callables_get[route] = api
        cls.module_mapping_get.setdefault(module, {})[route] = api

    # noinspection PyProtectedMember
    @classmethod
    def register_route_post(cls, route: str, async_method: AsyncApi, api: API, module: str) -> None:
        """
        Register the given handler as a POST call.  This should be called from the @web_api decorator, and
        rarely if ever called directly

        :param route: route to register under
        :param async_method: the raw handler for handling an incoming Request and returning a Response
        :param api: web api (decorated as @web_api) instance
        :param module: module that is holding the @web_api
        """
        if route in cls.routes_post or route in cls.routes_get:
            existing = cls.callables_get.get(route) or WebApplication.callables_post.get(route)
            raise cls.DuplicateRoute(f"Route '{route}' associated with {api.module}.{api._func.__name__}"
                                     f" already exists here {existing.module}.{existing.name} ")
        cls.routes_post[route] = async_method
        cls.callables_post[route] = api
        cls.module_mapping_post.setdefault(module, {})[route] = api

    # noinspection PyProtectedMember
    @classmethod
    def preprocess_module(cls, module_: str):
        if module_ not in sys.modules:
            mod = importlib.import_module(module_)
        else:
            mod = sys.modules.get(module_)
        for class_name, clazz in inspect.getmembers(mod, predicate=inspect.isclass):
            from .client import WebInterface
            if issubclass(clazz, WebInterface):
                ancestors = clazz.__mro__
                for ancestor in ancestors:
                    if ancestor == clazz:
                        continue
                    methods = inspect.getmembers(ancestor, predicate=inspect.ismethod) + \
                        inspect.getmembers(ancestor, predicate=inspect.isfunction)
                    for method_name, method in methods:
                        immediate = getattr(clazz, method_name)
                        if hasattr(immediate, '__func__'):
                            immediate = immediate.__func__
                        if hasattr(method, '_hydra_nano_services_web_api') and \
                                not hasattr(immediate, '_hydra_nano_services_web_api'):
                            web_api(content_type=method._hydra_nano_services_web_api.content_type,
                                    method=method._hydra_nano_services_web_api.method,
                                    is_constructor=method._hydra_nano_services_web_api.is_constructor,
                                    expire_obj=method._hydra_nano_services_web_api.expire_object,
                                    timeout=method._hydra_nano_services_web_api.timeout,
                                    uuid_param=method._hydra_nano_services_web_api.uuid_param)(immediate)
                            immediate._hydra_nano_services_web_api._clazz = clazz
                            # handles case of _expire !-> expire
                            immediate._hydra_nano_services_web_api._name = method._hydra_nano_services_web_api.name
                        if hasattr(method, '_hydra_nano_services_web_api'):
                            parameters1 = inspect.signature(method).parameters
                            if 'cls' in parameters1:
                                del parameters1['cls']
                            parameters2 = inspect.signature(method).parameters
                            if 'cls' in parameters1:
                                del parameters2['cls']
                            has_diff_web_api = (
                                method._hydra_nano_services_web_api.method !=
                                immediate._hydra_nano_services_web_api.method
                                or method._hydra_nano_services_web_api.content_type !=
                                immediate._hydra_nano_services_web_api.content_type
                                or method._hydra_nano_services_web_api.is_constructor !=
                                immediate._hydra_nano_services_web_api.is_constructor
                                or (inspect.signature(method).return_annotation !=
                                    inspect.signature(immediate).return_annotation)
                                or parameters1 != parameters2
                            )
                            if has_diff_web_api:
                                raise ValueError(
                                    f"Mismatch in @web_api specifications, parameter names/types or return type"
                                    f"in {class_name}.{method_name}"
                                    f" and {ancestor.__name__}.{method_name}"
                                )
                            elif method._hydra_nano_services_web_api.arg_annotations != \
                                    immediate._hydra_nano_services_web_api.arg_annotations:
                                raise TypeError(
                                    f"Mismatch in name and/or type specifications between {class_name}.{method_name}"
                                    f" and {ancestor.__name__}.{method_name}"
                                )

    # noinspection PyProtectedMember
    async def start(self,
                    modules: List[str],
                    host: Optional[str] = None,
                    port: Optional[int] = None,
                    sock: socket.socket = None,
                    path: Optional[str] = None,
                    initializer: Optional[Callable[[], None]] = None,
                    shutdown_timeout: float = 60.0,
                    ssl_context: Optional[SSLContext] = None,
                    backlog: int = 128,
                    handle_signals: bool = True,
                    reuse_address: Optional[bool] = None,
                    reuse_port: Optional[bool] = None,) -> None:
        """
        start the app

        :param modules: list of name of modules to import that contain the @web_api definitions to use, must not be
           empty and will be imported to load the @web_api definitions
        :param host: optional host of app, defaults to '127.0.0.1'
        :param port: optional port to listen on (TCP)
        :param path: path, if using UNIX domain sockets to listen on (cannot specify both TCP and domain parameters)
        :param initializer: optional function (no params, no return) to call on first bring-up, inside the
            thread associated with the app's asyncio loop
        :param shutdown_timeout: force shutdown if a shutdown call fails to take hold after this many seconds
        :param ssl_context: for HTTPS server; if not provided, will default to HTTP connection
        :param backlog: number of unaccepted connections before system will refuse new connections
        :param handle_signals: gracefully handle TERM signal or not
        :param reuse_address: tells the kernel to reuse a local socket in TIME_WAIT state, without waiting for its
           natural timeout to expire. If not specified will automatically be set to True on UNIX.
        :param reuse_port: tells the kernel to allow this endpoint to be bound to the same port as other existing
            endpoints are bound to, so long as they all set this flag when being created. This option is not supported
            on Windows.
        """
        # noinspection PyProtectedMember
        self._main_task = asyncio.create_task(self.MainThread.main(initializer))
        from aiohttp.web import _run_app as web_run_app
        for module in modules:
            self.preprocess_module(module)
            if module not in sys.modules:
                mod = importlib.import_module(module)
            else:
                mod = sys.modules.get(module)
            for api in list(self.module_mapping_get.get(module, {}).values()):
                if api.name in ['_expire', '_create']:
                    continue
                self._process_module_classes(mod, api)
            for api in self.module_mapping_post.get(module, {}).values():
                if api.name in ['_expire', '_create']:
                    continue
                self._process_module_classes(mod, api)
        allowed_get_routes = {}
        for module in [m for m in modules if m in self.module_mapping_get]:
            allowed_get_routes.update(self.module_mapping_get[module])
        for route, api_get in allowed_get_routes.items():
            log.debug(f">>>>>>>>>> GET ROUTE {route}")
            invoke = self.routes_get[route]
            self._web_app.router.add_get(route, wrap(self, invoke))
            self._all_apis.append(api_get)
        allowed_post_routes = {}
        for module in [m for m in modules if m in self.module_mapping_post]:
            allowed_post_routes.update(self.module_mapping_post[module])
        for route, api_post in allowed_post_routes.items():
            log.debug(f">>>>>>>>>> POST ROUTE {route}")
            invoke = self.routes_post[route]
            self._web_app.router.add_post(route, wrap(self, invoke))
            self._all_apis.append(api_post)
        if self._js_bundle_name:
            if self._static_path is None:
                raise ValueError("If 'js_bundle_name' is specified, 'static_path' cannot be None")
            static_path = Path(self._static_path)
            js_path = static_path.joinpath('js')
            if not js_path.exists():
                js_path.mkdir(parents=True)
            with open(js_path.joinpath(self._js_bundle_name + ".js"), 'bw') as out:
                if self._using_async:
                    JavascriptGeneratorAsync.generate(
                        out=out, skip_html=False,
                        allowed_routes=list(self.routes_get.keys()) + list(self.routes_post.keys())
                    )
                else:
                    JavascriptGenerator.generate(out=out, skip_html=False)
        self._started = True
        if self._static_path:
            with suppress(Exception):
                self._generate_rest_docs()
        if host is not None and sock is not None:
            raise ValueError("Cannot specify both host/port and sock parameters")
        await web_run_app(app=self._web_app, host=host, port=port, sock=sock, path=path,
                          shutdown_timeout=shutdown_timeout,
                          ssl_context=ssl_context, backlog=backlog, handle_signals=handle_signals,
                          reuse_address=reuse_address, reuse_port=reuse_port)

    async def shutdown(self) -> None:
        """
        Shutdown this server
        """
        if self._main_task is not None:
            self._main_task.cancel()
        if self._started:

            await self._web_app.shutdown()
            self._started = False

    # noinspection PyProtectedMember
    @classmethod
    def get_context(cls):
        i = 1
        frame = sys._getframe(i)
        try:
            # with debuggers, stack is polluted, so....
            while frame not in WebApplication._context:
                i += 1
                frame = sys._getframe(i)
        except IndexError:
            return None
        return WebApplication._context[frame].headers

    # noinspection PyProtectedMember
    def _process_module_classes(self, mod: types.ModuleType, api: API):
        # noinspection PyProtectedMember
        def process_class(clazz_: Type):
            if hasattr(clazz_, '_expire') or hasattr(clazz_, '_create'):
                raise TypeError("Classes with @web_api applied cannot have methods named _expire nor _create")

            # noinspection PyDecorator
            @staticmethod
            async def _create(*args, **kwargs) -> clazz_:
                if kwargs.get('__uuid') or (create_api.uuid_param is not None and create_api.uuid_param in kwargs):
                    self_id = kwargs[create_api.uuid_param] if create_api.uuuid_param is not None else kwargs['__uuid']
                    del kwargs['__uuid']
                    if self_id in self.ObjectRepo.instances:
                        raise HTTPException(404, f"UUid {self_id} already in use. uuid's must be unique")
                else:
                    self_id = str(uuid_pkg.uuid4())
                instance = clazz_(*args, **kwargs)
                if hasattr(clazz_, '__aenter__'):
                    await instance.__aenter__()
                self.ObjectRepo.instances[self_id] = instance
                return instance

            clazz_._create = _create
            if hasattr(clazz_, '__init__') and hasattr(clazz_.__init__, '__annotations__'):
                clazz_._create.__annotations__ = clazz_.__init__.__annotations__ if hasattr(clazz_, '__init__') else {}
            else:
                clazz._create.__annotations__ = {}

            clazz_._create.__doc__ = clazz_.__init__.__doc__ if hasattr(clazz_, '__init__') else f"""
                Create an instance of {clazz_.__name__} on server, returning a unique string id for the instance.
                The instance will remain active until /{clazz_.__name__}/expire is invoked.  The sting is used for
                instance-based ReST methods to act on the created instance on the server.

                :return: unique string id of instance created
                """
            clazz_._create.__qualname__ = f"{clazz_.__name__}._create"
            clazz._create.__module__ = clazz.__module__
            clazz_._create.__annotations__['return'] = clazz_
            _create.__func__.__annotations__ = clazz_._create.__annotations__
            _create.__annotations__ = clazz_._create.__annotations__
            # noinspection PyProtectedMember
            route_name = f"/{clazz_.__name__}/_create"
            create_api = API(
                content_type='text/plain',
                is_instance_method=False,
                is_class_method=False,
                is_constructor=True,
                method=RestMethod.GET,
                func=_create.__func__,
                clazz=clazz_
            )
            self.module_mapping_get.setdefault(clazz_.__module__, {})[route_name] = create_api
            self.routes_get[route_name] = clazz._create
            # self._all_apis[route_name] = api
            self._func_wrapper(clazz, clazz_._create, is_class_method=False, is_instance_method=False,
                               is_constructor=True,
                               expire_on_exit=False, method=RestMethod.GET, content_type="text/plain")

            async def _expire(this, new_lease_time: int = self.ObjectRepo.DEFAULT_OBJECT_EXPIRATION,
                              _uuid: Optional[str] = None) -> None:
                """
                Release/close an instance on the server that was created through invocation of _create for the
                associated resource
                """
                self_id = self.ObjectRepo.by_instance.get(this)
                if self_id not in self.ObjectRepo.instances:
                    raise HTTPException(code=404, msg=f"No such object with id {self_id}")
                self.ObjectRepo.expiry[self_id].cancel()
                if self.ObjectRepo.instances and new_lease_time <= 0:
                    await self.ObjectRepo.expire_obj(self_id, 0)
                else:
                    self.ObjectRepo.expiry[self_id] = asyncio.create_task(
                        self.ObjectRepo.expire_obj(self_id, new_lease_time))

            clazz_._expire = _expire
            clazz._expire.__module__ = clazz.__module__
            clazz_._expire.__doc__ = _expire.__doc__
            clazz_._expire.__name__ = 'expire'
            clazz_._expire.__qualname__ = f"{clazz_.__name__}.expire"

            # noinspection PyProtectedMember
            self._func_wrapper(clazz, clazz_._expire, is_class_method=False, is_instance_method=True,
                               expire_on_exit=False,
                               is_constructor=False, method=RestMethod.GET, content_type="text/plain")

        for clazz in [cl for _, cl in inspect.getmembers(mod) if inspect.isclass(cl)]:
            # noinspection PyProtectedMember
            method_found = any([item for item in inspect.getmembers(clazz) if item[1] == api._func])
            if method_found:
                api._clazz = clazz
            if method_found and (api in self._instance_methods or api.is_constructor):
                if clazz not in self._class_instance_methods or not self._class_instance_methods.get(clazz):
                    process_class(clazz)
                self._class_instance_methods.setdefault(clazz, []).append(api)
                self._instance_methods_class_map[api] = clazz
                if api.is_constructor:
                    if api._func.__annotations__.get('return') not in (clazz, clazz.__name__):
                        raise TypeError("@web_api's declared as constructors must return that class's type")

                if api.expire_object:
                    async def wrapped(this, *args, **kwargs):
                        try:
                            # noinspection PyProtectedMember
                            return await api._func(this, *args, **kwargs)
                        finally:
                            if this in self.ObjectRepo.by_instance:
                                uuid = self.ObjectRepo.by_instance[this]
                                del self.ObjectRepo.by_instance[this]
                                del self.ObjectRepo.instances[uuid]

                    setattr(clazz, api.name, wrapped)
            elif method_found and api in self._all_apis:
                if api.is_constructor:
                    if api._func.__annotations__.get('return') not in (clazz, clazz.__name__):
                        raise TypeError("@web_api's declared as constructors must return that class's type")
                self._class_instance_methods.setdefault(clazz, [])  # create an empty list of instance methods at least

    @classmethod
    def _func_wrapper(cls, clazz, func: WebApi,
                      is_instance_method: bool,
                      is_class_method: bool,
                      is_constructor: bool,
                      expire_on_exit: bool,
                      method: RestMethod,
                      content_type: str,
                      on_disconnect: Optional[Callable[[], None]] = None,
                      timeout: Optional[ClientTimeout] = None,
                      uuid_param: Optional[str] = None,
                      preprocess: Optional[PreProcessor] = None,
                      postprocess: Optional[PostProcessor] = None) -> WebApi:
        """
        Wraps a function as called from decorators.web_api to set up logic to invoke get or post requests

        :param func: function to wrap
        :param clazz: if function is instance method, provide owning class, otherwise None
        :return: function back, having processed as a web api and registered the route
        """
        api = API(clazz, func, method=method, content_type=content_type, is_instance_method=is_instance_method,
                  is_class_method=is_class_method, on_disconnect=on_disconnect,
                  is_constructor=is_constructor, expire_on_exit=expire_on_exit, uuid_param=uuid_param, timeout=timeout)
        func._hydra_nano_services_web_api = api
        if hasattr(func, '__func__'):
            func.__func__._hydra_nano_services_web_api = api
            func = func.__func__
        if is_instance_method:
            cls._instance_methods.append(api)
        if not inspect.iscoroutinefunction(func) and not inspect.isasyncgenfunction(func):
            raise ValueError("the @web_api decorator can only be applied to classes with public "
                             f"methods that are coroutines (async); see {api.qualname}")
        cls._all_methods.append(api)
        name = api.qualname.replace('__init__', 'create').replace('._expire', '.expire')
        name_parts = name.split('.')[-2:]
        route = '/' + '/'.join(name_parts)
        if '__isabstractmethod__' in func.__dict__:
            return func

        async def invoke(app: WebApplication, request: Request):

            async def invoke_proper():
                nonlocal preprocess, postprocess
                try:
                    preprocess = preprocess or app.preprocessor
                    try:
                        addl_args = (preprocess(request) or {}) if preprocess else {}
                    except Exception as e:
                        text = traceback.format_exc()
                        resp_ = Response(status=400, text=f"Error in preprocessing request: {e}\n{text}")
                        await resp_.prepare(request)
                        return resp_
                    if method == RestMethod.GET:
                        # noinspection PyProtectedMember
                        response = await cls._invoke_get_api_wrapper(
                            api, content_type=content_type, request=request, **addl_args
                        )
                    elif method == RestMethod.POST:
                        # noinspection PyProtectedMember
                        response = await cls._invoke_post_api_wrapper(
                            api, content_type=content_type, request=request, **addl_args
                        )
                    else:
                        raise ValueError(f"Unknown method {method} in @web-api")
                    try:
                        postprocess = postprocess or app.postprocessor
                        postprocess(response) if postprocess else response
                    except Exception as e:
                        text = traceback.format_exc()
                        resp_ = Response(status=400, reason=f"Exception in processing: {e}", text=text,
                                         content_type="text/plain")
                        await resp_.prepare(request)
                        return resp_
                    return response
                except PermissionError as e:
                    return await response_from_exception(
                        code=401,
                        request=request,
                        reason=f"PermissionError with request: {str(e)}"
                    )
                except TypeError as e:
                    return await response_from_exception(
                        code=400,
                        request=request,
                        reason=f"Improperly formatted query: {str(e)}"
                    )
                except HTTPException:
                    raise
                except (CancelledError, ConnectionResetError, ClientConnectionError):
                    raise
                except Exception as e:
                    return await response_from_exception(
                        request,
                        code=400,
                        reason=f"General exception in request: {str(e)}"
                    )
                except BaseException as e:
                    return await response_from_exception(
                        request,
                        code=500,
                        reason=f"General exception when processing request: {str(e)}"
                    )
            resp_q = asynciomultiplexer.AsyncAdaptorQueue(1)
            await cls.MainThread.start(invoke_proper(), resp_q)
            resp = await resp_q.get()
            if isinstance(resp, Exception):
                raise resp
            return resp

        invoke.clazz = WebApplication._instance_methods_class_map.get(api) if is_instance_method else None
        if method == RestMethod.GET:
            # noinspection PyProtectedMember
            WebApplication.register_route_get(route, invoke, api, api._real_func.__module__)
        elif method == RestMethod.POST:
            # noinspection PyProtectedMember
            WebApplication.register_route_post(route, invoke, api, api._real_func.__module__)
        else:
            raise ValueError(f"Unknown method {method} in @web-api")
        return func

    @classmethod
    async def _invoke_get_api_wrapper(cls, api: API, content_type: str, request: Request,
                                      **addl_args: Any) -> Union[Response, StreamResponse]:
        """
        Invoke the underlying GET web API from given request.  Called as part of setup in cls._func_wrapper

        :param func:  async function to be called
        :param content_type: http header content-type
        :param request: request to be processed
        :return: http response object
        """
        # noinspection PyUnresolvedReferences,PyProtectedMember
        cls._context[sys._getframe(0)] = request
        if api.has_streamed_request:
            raise TypeError("GET web_api methods does not support streaming requests")
        try:
            encoding = 'utf-8'
            items = content_type.split(';')
            for item in items:
                item = item.lower()
                if item.startswith('charset='):
                    encoding = item.replace('charset=', '')
            # report first param that doesn't match the Python signature:
            no_such_params = [p for p in request.query if p not in api.arg_annotations and p != 'self' and p != '']
            if no_such_params:
                resp = Response(
                    status=400,
                    text=f"No such parameter(s): {no_such_params} or missing type hint"
                         f" for param in method {api.qualname}"
                )
                await resp.prepare(request)
                return resp

            # convert incoming str values to proper type:
            kwargs = {k: _convert_request_param(v, api.arg_annotations[k]) if k != 'self' else v
                      for k, v in request.query.items() if k != ''}
            if addl_args:
                kwargs.update(addl_args)
            if api.is_instance_method:
                # we are invoking a class method, and need to lookup instance
                self_id = kwargs.get('self').replace('\\', '')
                if self_id is None:
                    raise ValueError(
                        "A self request parameter is needed. No instance provided for call to instance method")
                instance = cls.ObjectRepo.instances.get(self_id)
                if instance is None:
                    raise ValueError(f"No instance found for request with 'self' id of {self_id}")
                del kwargs['self']
                kwargs, varargs = api.update_varargs(kwargs)
                result = api(instance, *varargs, **kwargs)
            elif api.is_class_method:
                if isinstance(api.clazz, tuple):
                    module_name, class_name = api.clazz
                    api._clazz = getattr(sys.modules.get(module_name), class_name)
                kwargs, varargs = api.update_varargs(kwargs)
                if 'cls' not in kwargs:
                    result = api(api.clazz, *varargs, **kwargs)
                else:
                    result = api(*varargs, **kwargs)
            else:
                kwargs, varargs = api.update_varargs(kwargs)
                # call the underlying function:
                result = api(*varargs, **kwargs)
            if inspect.isasyncgen(result):
                #################
                #  streamed response through async generator:
                #################
                # underlying function has yielded a result rather than turning
                # process the yielded value and allow execution to resume from yielding task
                content_type = "text-streamed; charset=x-user-defined"
                response = StreamResponse(status=200, reason='OK', headers={'Content-Type': content_type})
                prepared = False
                # iterate to get the one (and hopefully only) yielded element:
                # noinspection PyTypeChecker
                try:
                    async for res in result:
                        if not prepared:
                            await response.prepare(request)
                            prepared = True
                            # This is done post-await of first result in cas of exception right off the bat
                        try:
                            serialized = _serialize_return_value(res, encoding)
                            if not isinstance(res, bytes):
                                serialized += b'\0'
                            await response.write(serialized)
                        except (CancelledError, ConnectionResetError, ClientConnectionError):
                            if api.on_disconnect is not None:
                                if inspect.iscoroutinefunction(api.on_disconnect) or\
                                    (hasattr(api.on_disconnect, '__func__') and
                                     inspect.iscoroutinefunction(api.on_disconnect.__func__)):
                                    await api.on_disconnect(res)
                                else:
                                    api.on_disconnect(res)
                            raise
                except (CancelledError, ConnectionError, ClientConnectionError):
                    raise
                except Exception as e:
                    if not prepared:
                        response.set_status(400, reason=f"Exception in request: {str(e)}")
                        await response.prepare(request)
                    else:
                        log.error(f"Exception in server-side logic handling request: {str(e)}")
                finally:
                    if not prepared:  # nothing generated in this case, but still a 200
                        await response.prepare(request)
                    await response.write_eof()
                    return response
            else:
                #################
                #  regular response
                #################
                result = await result
                instance = result
                if api.is_constructor:
                    if api.clazz and hasattr(api.clazz, '__aenter__'):
                        await instance.__aenter__()
                    if hasattr(api.clazz, 'jsonrepr'):
                        repr_ = api.clazz.jsonrepr(result)
                        uuid = repr_.get(api.uuid_param)
                        content_type = 'application/json'
                        result = json.dumps(repr_)
                    else:
                        uuid = kwargs.get(api.uuid_param, str(uuid_pkg.uuid4()))
                        result = json.dumps({'uuid': uuid})
                    cls.ObjectRepo.instances[uuid] = instance
                    cls.ObjectRepo.by_instance[instance] = uuid
                    cls.ObjectRepo.expiry[uuid] = asyncio.create_task(cls.ObjectRepo.expire_obj(
                        uuid, cls.ObjectRepo.DEFAULT_OBJECT_EXPIRATION))
                else:
                    result = _serialize_return_value(result, encoding)
                resp = Response(status=200, body=result if result is not None else b"Success",
                                content_type=content_type)
                await resp.prepare(request)
                try:
                    await resp.write_eof()
                except ConnectionError:
                    if api.on_disconnect is not None:
                        if inspect.iscoroutinefunction(api.on_disconnect) or \
                                (hasattr(api.on_disconnect, '__func__') and
                                 inspect.iscoroutinefunction(api.on_disconnect.__func__)):
                            await api.on_disconnect(result)
                        else:
                            api.on_disconnect(result)
                    raise
                return resp
        finally:
            # noinspection PyUnresolvedReferences,PyProtectedMember
            del cls._context[sys._getframe(0)]

    @classmethod
    async def _invoke_post_api_wrapper(cls, api: API, content_type: str, request: Request,
                                       **addl_args: Any) -> Union[Response, StreamResponse]:
        """
        Invoke the underlying POST web API from given request. Called as part of setup in cls._func_wrapper

        :param api:  API wrapping async function to be called
        :param content_type: http header content-type
        :param request: request to be processed
        :return: http response object
        """

        async def line_by_line_response(req: Request):
            reader = req.content
            chunk = True
            while not reader.is_eof() and chunk:
                chunk = await reader.readline()
                yield chunk
            last = await reader.readline()
            if last:
                yield last

        def streamed_bytes_arg_value(req: Request):
            async def iterator(packet_size: int):
                reader = req.content
                chunk = True
                while not reader.is_eof() and chunk:
                    chunk = await reader.read(packet_size)
                    yield chunk

            return iterator

        # noinspection PyUnresolvedReferences,PyProtectedMember
        cls._context[sys._getframe(0)] = request
        encoding = 'utf-8'
        items = content_type.split(';')
        for item in items:
            if item.startswith('charset='):
                encoding = item.replace('charset=', '')
        if not request.can_read_body:
            raise TypeError("Cannot read body for request in POST operation")
        try:
            kwargs: Dict[str, Any] = {}
            if api.has_streamed_request:
                key, typ = list(api.async_arg_annotations.items())[0]
                if typ == bytes:
                    kwargs = {key: await request.read()}
                elif typ == AsyncLineIterator:
                    kwargs = {key: line_by_line_response(request)}
                elif typ == AsyncChunkIterator:
                    kwargs = {key: streamed_bytes_arg_value(request)}
                for k, v in kwargs.items():
                    assert type(v) is str, f"key {k}={v} is of type {str(type(v))}\n{kwargs}"
                kwargs.update({k: _convert_request_param(v, api.synchronous_arg_annotations[k])
                               if k != 'self' else v
                               for k, v in request.query.items() if k in api.synchronous_arg_annotations})
            else:
                # treat payload as json string:
                bytes_response = await request.read()
                json_dict = json.loads(bytes_response.decode('utf-8'))
                for k in [p for p in json_dict if p not in api.arg_annotations and p != 'self']:
                    resp = Response(
                        status=400,
                        text=f"No such parameter or missing type hint for param {k} in method {api.qualname}"
                    )
                    await resp.prepare(request)
                    return resp

                # convert incoming str values to proper type:
                kwargs.update(dict(json_dict))
            # call the underlying function:
            if addl_args:
                kwargs.update(addl_args)
            for k, v in kwargs.items():
                if k != 'self':
                    value_type = api.arg_annotations[k]
                    kwargs[k] = normalize_from_json(v, value_type)
                else:
                    kwargs[k] = v
            if api.is_instance_method:
                self_id = kwargs.get('self')
                if self_id is None:
                    raise ValueError("No instance provided for call to instance method")
                instance = cls.ObjectRepo.instances.get(self_id)
                if instance is None:
                    try:
                        self_id = json.loads(self_id)['uuid']
                        instance = cls.ObjectRepo.instances.get(self_id)
                        if instance is None:
                            raise ValueError(f"No instance id found for request {self_id}")
                    except Exception:
                        raise ValueError(f"Error in json representation of an instance: {self_id}")
                del kwargs['self']
                kwargs, varargs = api.update_varargs(kwargs)
                awaitable = api(instance, *varargs, **kwargs)
                if api.expire_object:
                    del cls.ObjectRepo.instances[self_id]
            elif api.is_class_method:

                if isinstance(api.clazz, tuple):
                    module_name, class_name = api.clazz
                    api._clazz = getattr(sys.modules.get(module_name), class_name)
                kwargs, varargs = api.update_varargs(kwargs)
                awaitable = api(api.clazz, *varargs, **kwargs)
            else:
                kwargs, varargs = api.update_varargs(kwargs)
                awaitable = api(*varargs, **kwargs)
            if inspect.isasyncgen(awaitable):
                #################
                #  streamed response through async generator:
                #################
                # underlying function has yielded a result rather than turning
                # process the yielded value and allow execution to resume from yielding task
                # async_q = asyncio.Queue()
                content_type = "text/streamed; charset=x-user-defined"
                response = StreamResponse(status=200, reason='OK', headers={'Content-Type': content_type})
                prepared = False
                try:
                    # iterate to get the one (and hopefully only) yielded element:
                    # noinspection PyTypeChecker
                    count = 0
                    async for res in awaitable:
                        try:
                            serialized = _serialize_return_value(res, encoding)
                            if not isinstance(res, bytes):
                                serialized += b'\0'
                            if not prepared:
                                await response.prepare(request)
                            await response.write(serialized)
                            count += 1
                        except (CancelledError, ConnectionResetError, ClientConnectionError):
                            if api.on_disconnect is not None:
                                try:
                                    # noinspection PyUnboundLocalVariable
                                    args = (instance, res, ) if api.is_instance_method else (res, )
                                    if inspect.iscoroutinefunction(api.on_disconnect) or\
                                        (hasattr(api.on_disconnect, '__func__') and
                                         inspect.iscoroutinefunction(api.on_disconnect.__func__)):
                                        await api.on_disconnect(*args)
                                    else:
                                        api.on_disconnect(*args)
                                except Exception as e:
                                    print(f"ERROR in call to on_disconnect from "
                                          f"hydra.nano_services async generator: {e}")
                            break
                except (CancelledError, ConnectionResetError, ClientConnectionError):
                    raise
                except Exception as e:
                    logging.error(str(e))
                    if not prepared:
                        response = Response(status=400, reason=f"Exception in servicing request: {e}",
                                            body=f"Exception in servicing request: {e}")
                        await response.prepare(request)
                    else:
                        await response.write(f"Exception in server-side logic: {e}".encode('utf-8'))
                        with suppress(Exception):
                            await response.write_eof()
                finally:
                    if not prepared:
                        await response.prepare(request)
                    await response.write_eof()
                return response
            else:
                #################
                #  regular response
                #################
                if api.is_constructor:
                    instance = await awaitable
                    if api.clazz and hasattr(api.clazz, '__aenter__'):
                        await instance.__aenter__()
                    if hasattr(api.clazz, 'jsonrepr'):
                        repr_ = api.clazz.jsonrepr(instance)
                        uuid = repr_.get(api.uuid_param)
                        content_type = 'application/json'
                        result = json.dumps(repr_)
                    else:
                        uuid = kwargs.get(api.uuid_param, str(uuid_pkg.uuid4()))
                        result = json.dumps({'uuid': uuid})
                    cls.ObjectRepo.instances[uuid] = instance
                    cls.ObjectRepo.by_instance[instance] = uuid
                    cls.ObjectRepo.expiry[uuid] = asyncio.create_task(cls.ObjectRepo.expire_obj(
                        uuid, cls.ObjectRepo.DEFAULT_OBJECT_EXPIRATION))
                    resp = Response(status=200, body=result if result is not None else b"Success",
                                    content_type=content_type)
                    await resp.prepare(request)
                    return resp
                else:
                    result = _serialize_return_value(await awaitable, encoding)
                resp = Response(status=200, body=result if result is not None else b"Success",
                                content_type=content_type)
                await resp.prepare(request)
                return resp
        finally:
            # noinspection PyUnresolvedReferences,PyProtectedMember
            del cls._context[sys._getframe(0)]


def _convert_request_param(value: str, typ: Type) -> Any:
    """
    Convert rest request string value for parameter to given Python type, returning an instance of that type

    :param value: value to convert
    :param typ: Python Type to convert to
    :return: converted instance, of the given type
    :raises: TypeError if value can not be converted/deserialized
    """
    try:
        return from_str(value, typ)
    except Exception as e:
        text = traceback.format_exc()
        raise TypeError(f"Converting web request string {value} of {type(value)} to Python type {typ}: {e}\n {text}")


def _serialize_return_value(value: Any, encoding: str) -> bytes:
    """
    Serialize a Python value into bytes to return through a Rest API.  If a basic type such as str, int or float, a
    simple str conversion is done, then converted to bytes.  If more complex, the conversion will invoke the
    '__str__' method of the value, raising TypeError if such a method does not exist or does not have a bare
    (no-parameter) signature.

    :param value: value to convert
    :return: bytes serialized from value
    """
    try:
        if value is None:
            return bytes()
        return to_str(value).encode(encoding)
    except Exception as e:
        raise TypeError(f"Converting response '{value}' from Python type '{type(value)}' to string: {e}")
