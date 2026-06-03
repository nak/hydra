import importlib
import sys

from hydra.nano_services.http import WebApplication


def _name(typ):
    if typ is None or isinstance(typ, type(None)):
        return 'None'
    if hasattr(typ, '__name__'):
        return typ.__name__
    elif hasattr(typ, '__qualname__'):
        return typ.__qualname__
    module = typ.__module__
    return str(typ)[len(module) + 1:].replace('NoneType', 'None')


def main():
    if len(sys.argv) not in (2, 3):
        print(f"Usage: {sys.argv[0]} <module-name> [suffix-for-client-class]")
        sys.exit(1)
    module_name = sys.argv[1]
    class_suffix = sys.argv[2] if len(sys.argv) == 3 else ''
    # load the web_apis:
    importlib.import_module(module_name)
    apis = WebApplication.module_mapping_get.get(module_name, {})
    apis.update(
        WebApplication.module_mapping_post.get(module_name, {})
    )
    if not apis:
        print(f"No web_apis defined in {module_name}")
        sys.exit(2)
    by_class = {}
    for route, api in apis.items():
        class_name = route[1:].split('/')[0]
        assert class_name, f"Route has improper spec: '{route}'"
        by_class.setdefault(class_name, {})[route] = api

    text = """
import typing

from hydra.nano_services.client import WebInterface
from abc import abstractmethod
from hydra.nano_services.http import web_api
from hydra.nano_services.api import RestMethod
"""
    imports = set()
    for class_name, route_mapping in by_class.items():
        text += f"""

class {class_name}Interface(WebInterface):

    """
        for route, api in route_mapping.items():
            target = route[1:].split('/', maxsplit=1)[1]
            args = api.arg_annotations
            arg_text = ', '.join(f"{arg_name}: {_name(typ)}" for arg_name, typ in args.items())
            for typ in list(args.values()) + [api.return_type]:
                if typ is None:
                    continue
                elif typ.__module__ != 'builtins':
                    imports.add(f"from {typ.__module__} import {_name(typ).split('[', maxsplit=1)[0]}")
            web_api_args = \
                f"method={api.method}, content_type='{api.content_type}', "
            if api.is_constructor:
                web_api_args += "is_constructor=True, "
            if api.uuid_param is not None:
                web_api_args += f"uuid_param=\"{api.uuid_param}\""
            return_type = f"{_name(api.return_type)}"
            if api.has_streamed_response:
                return_type = f"AsyncIterator[{return_type}]"
                imports.add('from typing import AsyncIterator')
            if api.is_static:
                addl_decorator = "@staticmethod\n    "
            elif api.is_class_method:
                addl_decorator = "@classmethod\n    "
                arg_text = f"cls, {arg_text}"
            else:
                addl_decorator = ''
            if api.is_instance_method:
                arg_text = f"self, {arg_text}"
            assert web_api_args
            text += f"""
    @web_api({web_api_args})
    {addl_decorator}@abstractmethod
    async def {target}({arg_text}) -> {return_type}:
        \"\"\"
        abstraction for {route} web-api route
        \"\"\"
    """
        text += f"""

{class_name}{class_suffix} = {class_name}Interface.Client()
        """
    sys.stdout.write('\n'.join(imports) + '\n')
    sys.stdout.write(text)
    sys.stdout.write('\n')
    return 0


if __name__ == '__main__':
    main()
