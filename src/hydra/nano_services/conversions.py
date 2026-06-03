"""
Since *hydra.nano_services* is essentially an RPC mechanism, all ``@web_api`` methods must be
strictly type-annotated. Only a restricted set of types is allowed -- specifically, ones that
can be JSON-serialized and deserialized. The following types are supported:

#. ``dict``, ``typing.Dict``, ``frozendict``, ``collections.OrderedDict``, ``typing.OrderedDict``,
   ``collections.abc.Mapping``
#. ``list``, ``set``, ``frozenset``, ``FrozenList``, ``tuple``, ``typing.List``, ``typing.Set``, ``typing.Tuple``
#. ``str``, ``int``, ``float``, ``bool``, ``bytes``
#. ``datetime.datetime``, ``uuid.UUID``, ``pathlib.Path``, ``pathlib.PosixPath``, ``pathlib.WindowsPath``
#. *@dataclasses*
#. *enum.Enum*'s, provided the *Enum*'s value has a type which falls within these rules
#. Classes that implement the pickle interface methods ``__getstate__`` and ``__setstate__``
#. Recursive combinations of the above (e.g., a list of dicts of dataclasses, etc.)

The rule of thumb is that, in the end, a type must resolve to basic types convertible for use in
JSON serialization. In practice, the APIs should be designed with the simplest of types
possible. The intent is to allow the interfaces to be accessible, in principle, to any language.

Types that are **not** supported include:

* Arbitrary objects that do not implement ``__getstate__`` / ``__setstate__`` and are not dataclasses
* Generators, iterators, and coroutines (except where ``AsyncIterator`` / ``AsyncGenerator`` are
  used as the streaming return type)
* File handles, sockets, and other OS resources
* Callables, lambdas, and bound methods

Because the interface and concrete server implementation are validated independently, the
annotations and parameter names in each interface ``@web_api`` declaration must match those of
the concrete server implementation. Both must be kept in sync whenever the web API changes.

Streaming Responses
~~~~~~~~~~~~~~~~~~~

Some APIs naturally produce a sequence of items over time rather than a single response --
for example, a long-running computation reporting progress, a tail of log lines, or a
chunked download. *hydra.nano_services* supports this pattern by allowing the return type of
a ``@web_api`` method to be declared as ``typing.AsyncIterator[T]`` or
``typing.AsyncGenerator[T, None]``, where ``T`` is any of the supported types listed above.

When such a return type is declared, the framework:

#. forces the HTTP response content type to ``text/streamed; charset=x-user-defined`` so the
   transport can deliver items as they are produced;
#. on the server, awaits each value yielded by the implementation and serializes it
   individually, flushing it onto the response stream as soon as it is available;
#. on the Python client side, exposes the proxy method as an async iterator so the caller
   can ``async for`` over the items as they arrive;
#. on the auto-generated JavaScript client side, generates an ``async`` generator function
   (``async*``) that yields each item to browser code as it arrives.

A streaming server method is implemented as a normal Python ``async`` generator -- yielding
each item with ``yield`` -- and is consumed at the client with ``async for``:

.. code-block:: python

    # on the server (concrete implementation)
    @classmethod
    @web_api(content_type='application/json', method=RestMethod.GET)
    async def tail_progress(cls, job_id: str) -> AsyncIterator[int]:
        \"\"\"
        Stream progress percentages for a running job.

        :param job_id: id of the job to tail
        :return: a stream of integer progress values (0--100)
        \"\"\"
        while not job_done(job_id):
            yield current_progress(job_id)
            await asyncio.sleep(1.0)

    # on the client
    async for percent in ProgressProxy.tail_progress(job_id="abc123"):
        print(percent)

The same supported-types rules apply to the *item* type ``T`` -- for example,
``AsyncIterator[MyDataclass]`` is valid, but ``AsyncIterator[<unsupported type>]`` is not.
The ``content_type`` argument passed to ``@web_api`` is ignored for streaming responses;
the framework selects the streaming content type automatically.

.. note::

    A complementary mechanism exists for streaming *request* bodies into a server method:
    a single parameter may be annotated as ``hydra.nano_services.api.AsyncChunkIterator``
    (for raw byte chunks) or ``hydra.nano_services.api.AsyncLineIterator`` (for newline-
    delimited text). At most one such streaming parameter is allowed per ``@web_api``.  Not
    all browsers support this, particularly if used with a streaming response, so use with caution.
"""
import collections
import datetime
import inspect
import json
import types
import typing
import uuid
from pathlib import Path, WindowsPath, PosixPath
from typing import Type, Any, FrozenSet
from enum import Enum

from frozendict import frozendict
from frozenlist import FrozenList

assert typing


def is_dataclass(val: Any) -> bool:
    return hasattr(val, '__dataclass_fields__')


def normalize_to_json_compat(val: Any) -> Any:
    if val is None:
        return None
    if is_dataclass(val):
        json_data = {k: v for k, v in val.__dict__.items()
                     if not inspect.ismethod(v)}
        for key in json_data.keys():
            if json_data[key] is not None:
                json_data[key] = normalize_to_json_compat(json_data[key])
    elif isinstance(val, Enum):
        json_data = normalize_to_json_compat(val.value)
    elif type(val) in (str, int, float, bool):
        json_data = val
    elif type(val) in (datetime.datetime, ):
        json_data = val.isoformat()
    elif type(val) in (uuid.UUID, PosixPath, WindowsPath, Path):
        json_data = str(val)
    elif type(val) in (collections.OrderedDict, ) or getattr(type(val), '_name', None) in ('OrderedDict', )\
            or getattr(type(val), '__name__', None) in ('OrderedDict', ):
        json_data = []
        for key, value in val.items():
            json_data.append([to_str(key), normalize_to_json_compat(value)])
    elif type(val) in [dict, frozendict] or (getattr(type(val), '_name', None) in ('Dict', 'Mapping', )):
        json_data = {}
        for key, value in val.items():
            json_data[to_str(key)] = normalize_to_json_compat(value)
    elif type(val) in [list, set, bytes, tuple, frozenset, FrozenList]\
            or (getattr(type(val), '_name', None) in ('List', 'Set' 'Tuple'))\
            or getattr(type(val), '__name__', None) in ('frozenset', 'FrozenList'):
        json_data = []
        for value in val:
            json_data.append(normalize_to_json_compat(value))
    else:
        raise TypeError(f"Unsupported type for conversion and type is not pickable: {type(val)}")
    return json_data


def type_origin(annotation: Any) -> Any:
    if hasattr(annotation, '__origin__'):
        return annotation.__origin__
    else:
        return annotation


def full_type(annotation: Any) -> tuple[Any, tuple[Type, ...]]:
    base = type_origin(annotation)
    type_args = annotation.__args__ if hasattr(annotation, '__args__') else tuple()
    # noinspection PyBroadException
    if base == typing.Union or type(base) is types.UnionType:
        return types.UnionType, type_args
    elif base == typing.Optional:
        return types.UnionType, (base.__args__[0], types.NoneType)
    elif _issubclass_safe(annotation, Enum):
        return annotation, tuple()
    elif base in (str, int, float, bool, datetime.datetime, uuid.UUID, PosixPath, WindowsPath, Path):
        return base, tuple()
    elif base == dict or base == frozendict or base in (typing.OrderedDict, collections.OrderedDict):
        return base, type_args
    elif annotation in (collections.abc.Mapping, ):
        return dict, type_args
    elif base in (list, set, bytes, frozenset, FrozenList, tuple):
        return base, type_args
    elif hasattr(annotation, '__dataclass_fields__'):
        return annotation, tuple()
    elif hasattr(annotation, '__getstate__') and hasattr(annotation, '__setstate__'):
        return annotation, tuple()
    else:
        raise TypeError(f"Unsupported type for web api: '{annotation}'.  "
                        "Parameterizable types must include type specifics")


def normalize_from_json(json_data, typ: Any) -> Any:
    if typ is None or json_data is None:
        return None
    # noinspection PyBroadException
    try:
        if isinstance(json_data, str) and str(typ(json_data)) == json_data:
            return typ(json_data)
    except Exception:
        pass
    true_type, type_args = full_type(typ)
    # noinspection PyBroadException
    try:
        if true_type == types.UnionType:
            if type(json_data) in type_args:
                return json_data
            for type_arg in type_args:
                if json_data == '' and type_arg is types.NoneType:
                    return None
                else:
                    # noinspection PyBroadException
                    try:
                        return normalize_from_json(json_data, type_arg)  # recurse on arg type
                    except Exception:
                        # no type match, try nex type for conversion
                        continue
            raise TypeError(f"Cannot convert json data '{json_data}' to any of the Union types '{typ}'")
        elif _issubclass_safe(true_type, Enum):
            for key in true_type.__members__:  # type: ignore
                t = type(true_type.__members__[key].value)  # type: ignore
                # noinspection PyBroadException
                try:
                    v = normalize_from_json(json_data, t)
                    return true_type(v)
                except Exception:
                    continue
            return true_type(json_data)
        elif true_type in (uuid.UUID, Path, str, int, float, PosixPath, WindowsPath, Path):
            return true_type(json_data)
        elif true_type in (datetime.datetime, ):
            return datetime.datetime.fromisoformat(json_data)
        elif true_type == bool:
            return str(json_data).lower() == 'true'
        elif true_type in (typing.OrderedDict, collections.OrderedDict):
            # return list of tuples of key/value, as order must be preserved
            key_typ, elem_typ = type_args
            return true_type([
                (normalize_from_json(from_str(item[0], key_typ), key_typ),  normalize_from_json(item[1], elem_typ))
                for item in json_data
            ])
        elif true_type in (dict, frozendict):
            try:
                key_typ, elem_typ = type_args
            except ValueError:
                raise TypeError(f"Unsupported dict type for web api: '{typ}'."
                                " Type args has too many value specifications")
            try:
                return true_type({
                    normalize_from_json(from_str(k, key_typ), key_typ):  normalize_from_json(v, elem_typ)
                    for k, v in json_data.items()
                })
            except Exception:
                raise TypeError(f"Unsupported dict type for web api: '{typ}'.  ")
        elif true_type in (list, FrozenList, FrozenSet, frozenset, set, bytes):
            return true_type([normalize_from_json(value, type_args[0]) for index, value in enumerate(json_data)])
        elif true_type == tuple:
            if typ.__args__[-1] == Ellipsis:
                args = typ.__args__[:-1]
                have_ellipsis = True
            else:
                have_ellipsis = False
                args = typ.__args__
            if len(json_data) < len(args) or (not have_ellipsis and len(args) != len(json_data)):
                raise ValueError(f"Too few elements in json data for tuple type {typ}")
            for index in range(len(args), len(json_data)):
                if not have_ellipsis:
                    # already checked?
                    raise ValueError(f"Too many elements in json data for tuple type {typ}")
                args += (args[-1], )  # fill in ellipsis types
            return tuple(normalize_from_json(value, args[index]) for index, value in enumerate(json_data))
        elif hasattr(typ, '__dataclass_fields__'):
            return typ(**{
                name: normalize_from_json(json_data[name], field.type)
                for name, field in typ.__dataclass_fields__.items()
            })
        elif hasattr(typ, '__setstate__'):
            result = typ.__new__(typ)
            result.__setstate__(json_data)
            return result
        else:
            raise TypeError
    except Exception as e:
        raise TypeError(f"Unsupported typ for web api: '{typ}' [{e}]")


def to_str(val: Any) -> str | None:
    if val is None:
        return None
    true_type, _ = full_type(type(val))
    if true_type == bool:
        return str(val).lower()
    # noinspection PyBroadException
    try:
        if true_type(str(val)) == val:
            return str(val)
    except Exception:
        pass
    if hasattr(val, '__dataclass_fields__'):
        val = normalize_to_json_compat(val)
        return json.dumps(val)
    elif isinstance(val, Enum):
        return to_str(val.value)
    elif true_type in (uuid.UUID, str, int, float, PosixPath, WindowsPath, Path):
        return str(val)
    elif true_type in (datetime.datetime, ):
        return val.isoformat()
    elif true_type in (dict, collections.OrderedDict):
        val = normalize_to_json_compat(val)
        return json.dumps(val)
    elif true_type in (list, bytes, set, frozenset, tuple, FrozenList):
        val = normalize_to_json_compat(list(val))
        return json.dumps(val)
    elif hasattr(val, '__getstate__'):
        if not hasattr(true_type, '__setstate__'):
            raise TypeError(f"Unsupported: class {true_type} "
                            "has __getstate__ method without matching __setstate__ method")
        return to_str(val.__getstate__())  # recurse on pickle-able state of the object
    else:
        raise TypeError(f"Type of value, '{type(val)}' is not supported in web api")


def _issubclass_safe(typ, clazz):
    # noinspection PyBroadException
    try:
        return issubclass(typ, clazz)
    except Exception:
        return False


def from_str(image: str, typ: Any) -> Any:
    true_type, type_args = full_type(typ)
    if true_type == types.UnionType:
        for type_arg in type_args:
            try:
                if not image and type_arg is types.NoneType:
                    return None
                return from_str(image, type_arg)
            except (ValueError, TypeError):
                continue
        raise TypeError(f"Cannot convert string '{image}' to any of the Union types '{typ}'")
    #######
    if _issubclass_safe(true_type, Enum):
        return true_type(image)
    elif true_type in (uuid.UUID, str, int, float, PosixPath, WindowsPath, Path):
        return true_type(image)
    elif true_type in (datetime.datetime, ):
        return datetime.datetime.fromisoformat(image)
    elif typ == bool:
        return image.lower() == 'true'
    elif true_type in (dict, frozendict, typing.OrderedDict, set, tuple, list, bytes,  FrozenList, frozenset,
                       collections.OrderedDict, collections.abc.Mapping) or \
            hasattr(true_type, '__dataclass_fields__'):
        return normalize_from_json(json.loads(image), typ)
    elif typ is None:
        if image:
            raise ValueError(f"Got a return of {image} for a return type of None")
        return None
    elif hasattr(true_type, '__setstate__'):
        result = true_type.__new__(true_type)
        result.__setstate__(json.loads(image))
        return result
    else:
        raise TypeError(f"Unsupported typ for web api: '{typ}'")
