"""
hydra.nano_services provides the ability to auto-generate client-side javascript code to abstract the details of makting
HTTP requests to the server.  This abstraction implies that the developer never need to know about routes,
formulating URLs, how to make a POST request, how to stream data over HTTP, etc!

To generate client side code, create a Python source file -- let's name it generator.py --
and import all of the classes containing @web_api's
to be generated. Then add the main entry poitn to generate the javascript code, like so:

>>> from hydra.nano_services.js_async import JavascriptGeneratorAsync
... from salutations import Greetings  # noqa
...
... if __name__ == '__main__':
...     with open('salutations.js', 'bw') as output:
...        JavascriptGeneratorAsync.generate(out=output, skip_html=True)

Run the script as so:

.. code-block:: bash

    % python generate.py

With the above Greetings example, the generated javasctipt code will mimic the Python:

.. code-block:: javascript
    :caption: javascript code auto-generated from Python server's web API

    class hydra{}; hydra.nano_services = class{};
    hydra.nano_services.salutations = class {};
    hydra.nano_services.salutations.Greetings = class {

          /*
          Welcome someone
          The response will be provided as the (first) parameter passed into onsuccess

          @param {{string}} name name of person to greet
          */
          welcome(name) {
             ...
          }

           /*
           Tell someone goodbye by telling them to have a day (of the given type)
           The response will be provided as the (first) parameter passed into onsuccess

           @param {{string}} type_of_day adjective describing type of day to have
           */
          goodbye(type_of_day) {
            ...
          }
    };

The code maintains the same hierarchy in namespaces as modules in Python, albeit under a global *hydra.nano_services* namespace.
This prevents potential namespace collisions.  The javascript API mimics the signature of the Python API, including
async generators/iterator usage.


"""
import datetime
import inspect
import sys
import uuid

from aiohttp.web_response import Response, StreamResponse
from typing import Callable, Awaitable, Union
from typing import Dict, Tuple, List, IO
from urllib.request import Request

from hydra.nano_services.api import API, APIDoc, RestMethod

AsyncApi = Callable[[Request], Awaitable[Union[Response, StreamResponse]]]


class JavascriptGeneratorAsync:
    """
    Class for generating equivalent Javascript API from Python web_api routes
    """

    ENCODING = 'utf-8'

    HYDRA_NANO_SERVICES_CORE = """

class HYDRA_NANO_SERVICES_UUID{
    constructor(uuid){
        this.uuid = uuid;
    }
}

class hydra{};
hydra.nano_services = class{

    static split_string(str){
        let values = str.split('\\0');
        return [values.slice(0, -1), values.slice(-1)[0]];
        
    }

    static compute_query(param_map){
        let c = '?';
        let params = '';
        for (var param in param_map){
             if (typeof param_map[param] !== 'undefined'){
                 let value = JSON.stringify(param_map[param])
                 if (typeof value === 'undefined'){
                    continue;
                 }
                 if (value[0] === '"'){
                    value = value.slice(1, -1)
                 }
                 params += c + param + '=' + value;
                 c= '&';
             }
        }
        return params;
    }

    static async fetch_GET(route, content_type, param_map, convert){
        let result = await fetch(route + hydra.nano_services.compute_query(param_map),
                    {method:'GET', headers: {'Content-Type': content_type}});
        if (result.status < 200 || result.status > 299){
            let statusBody = await result.body.getReader().read();
            statusBody = result.statusText + ": " + new TextDecoder().decode(statusBody.value);
            let stats = {status: result.status, statusText: statusBody};
            throw stats;
        }
        return convert(await result.text());
    }

    static async *fetch_GET_streamed(route, content_type, param_map, convert, return_is_bytes){
        let result = await fetch(route + hydra.nano_services.compute_query(param_map),
                                 {method:'GET', duplex: 'half', headers: {'Content-Type': content_type}});
        let reader = await result.body.getReader();
        let left_over = "";
        while (true){
            if (result.status < 200 || result.status > 299){
                let statusBody = await reader.read();
                statusBody = result.statusText + ": " + new TextDecoder().decode(statusBody.value);
                let stats = {status: result.status, statusText: statusBody};
                throw stats;
            }
            let resp = await reader.read();
            if (resp.done){
                break;
            }
            if (return_is_bytes){
               yield resp.value;
            } else {
               let value = new TextDecoder().decode(resp.value);
               value = left_over + value;
               let [items, last_chunk] = hydra.nano_services.split_string(value);
               left_over = last_chunk;
               for (var val of items){
                   if(val){
                      yield convert(val);
                   }
               }
           }
        }
    }

    static async fetch_POST(route, content_type, param_map, convert, streamed_param){
        let params;
        let requestBody;
        if (typeof streamed_param !== 'undefined'){
            params = hydra.nano_services.compute_query(param_map);
            requestBody = new ReadableStream({
                async start(controller){
                    let encoder = new TextEncoder();
                    for await (var chunk of streamed_param){
                        controller.enqueue(encoder.encode(chunk));
                    }
                    controller.close();
                }
            });
        } else {
            params = '';
            requestBody = JSON.stringify(param_map);
        }
        try{
            let result = await fetch(route + params,
                               {method:'POST', headers: {'Content-Type': content_type},
                                body: requestBody});

            if (result.status < 200 || result.status > 299){
                let statusBody = await result.body.getReader().read();
                statusBody = result.statusText + ": " + new TextDecoder().decode(statusBody.value);
                let stats = {status: result.status, statusText: statusBody};
                throw stats;
            }
            return convert(await result.text());
        } catch (error) {
            if (error.message === 'Failed to fetch'){
                throw new Error("Streamed requests not supported by this browser");
            }
            throw error;
        }
    }

    static async * fetch_POST_streamed(route, content_type, param_map, convert, return_is_bytes, streamed_param){
        let body;
        if (typeof streamed_param !== 'undefined'){
            let requestBody = new ReadableStream({
                async start(controller){
                    let encoder = new TextEncoder();
                    for await (var chunk of streamed_param){
                        controller.enqueue(encoder.encode(chunk));
                    }
                    controller.close();
                }
            });
            try{
                body = await fetch(route + hydra.nano_services.compute_query(param_map), {method:'POST',
                                   duplex: 'half', headers: {'Content-Type': content_type},
                                   body: requestBody});
            } catch (error) {
                if (error.message === 'Failed to fetch'){
                    throw new Error("Streamed requests not supported by this browser");
                }
                throw error;
            }
        } else {
            body = await fetch(route, {method:'POST', headers: {'Content-Type': content_type},
                                       duplex: 'half', body: JSON.stringify(param_map)});
        }
        let reader = await body.body.getReader();
        let left_over = "";
        while (true){
            if (body.status < 200 || body.status > 299){
                let statusBody = await reader.read();
                statusBody = body.statusText + ": " + new TextDecoder().decode(statusBody.value);
                let stats = {status: body.status, statusText: statusBody};
                throw stats;
            }
            let resp = await reader.read();
            if (resp.done){
                break;
            }
            if (return_is_bytes){
               yield resp.value;
            } else {
               let value = new TextDecoder().decode(resp.value);
               value = left_over + value;
               let [items, last_chunk] = hydra.nano_services.split_string(value);
               left_over = last_chunk;
               for (var val of items){
                   if(val){
                      yield convert(val);
                   }
               }
           }
        }
    }

    static convert_int(text){
        let converted = parseInt(text);
        if (typeof converted === 'numbered' && isNaN(buffered)){
              let stats ={status:-1, statusText:"Unable to convert server response '" + val + "' to int"};
              throw stats;
         }
         return converted;
    }

    static convert_str(text){
        return text;
    }
    
    static convert_datetime(text){
       return new Date(text);
    }
    
    static convert_bytes(text){
        let encoder = new TextEncoder();
        return encoder.encode(text);
    }

    convert_float(text){
        let converted = parseFloat(text);
        if (typeof converted === 'numbered' && isNaN(buffered)){
             let stats = {status:-1, statusText:"Unable to convert server response '" + val + "' to float"};
             throw stats;
         }
         return converted;
    }

    static convert_bool(text){
        return text === 'true';
    }

    static convert_None(text){
        return null;
    }

    static convert_complex(text){
        return JSON.parse(text);
    }

};
"""

    class Namespace:

        def __init__(self):
            self._namespaces: Dict[str, JavascriptGeneratorAsync.Namespace] = {}
            self._classes: Dict[str, List[Tuple[RestMethod, str, API]]] = {}

        def add_module(self, module: str) -> 'JavascriptGeneratorAsync.Namespace':
            if '.' in module:
                my_name, child = module.split('.', maxsplit=1)
                m = self._namespaces.setdefault(my_name, JavascriptGeneratorAsync.Namespace())
                m = m.add_module(child)
            else:
                my_name = module
                m = self._namespaces.setdefault(my_name, JavascriptGeneratorAsync.Namespace())
            return m

        def add_class_and_route_get(self, class_name: str, route: str, api: API) -> None:
            m = self.add_module(api.module)
            m._classes.setdefault(class_name, []).append((RestMethod.GET, route, api))

        def add_class_and_route_post(self, class_name: str, route: str, api: API):
            m = self.add_module(api.module)
            m._classes.setdefault(class_name, []).append((RestMethod.POST, route, api))

        @property
        def child_namespaces(self):
            return self._namespaces

        @property
        def classes(self):
            return self._classes

    @classmethod
    def generate(cls, allowed_routes: List[str], out: IO, skip_html: bool = True) -> None:
        """
        Generate javascript code from registered routes

        :param out: stream to write to
        :param skip_html: whether to skip entries of content type 'text/html' as these are generally not used in direct
           javascript calls
        """
        from .http import WebApplication
        namespaces = cls.Namespace()
        for route, api in WebApplication.callables_get.items():
            if route not in allowed_routes:
                continue
            if not skip_html or (api.content_type.lower() != 'text/html'):
                classname = route[1:].split('/')[0]
                namespaces.add_class_and_route_get(classname, route, api)
        for route, api in WebApplication.callables_post.items():
            if route not in allowed_routes:
                continue
            if not skip_html or (api.content_type.lower() != 'text/html'):
                classname = route[1:].split('/')[0]
                namespaces.add_class_and_route_post(classname, route, api)
        tab = ""

        def process_namespace(ns: cls.Namespace, parent_name: str):
            nonlocal tab
            for name_, child_ns in ns.child_namespaces.items():
                out.write(f"{parent_name}.{name_} = class {{}}\n".encode(cls.ENCODING))
                process_namespace(child_ns, parent_name + '.' + name_)
            clazz_map = {c.__name__: c for c in WebApplication._class_instance_methods
                         if WebApplication._class_instance_methods[c]}
            for class_name, routes in ns.classes.items():
                if class_name in clazz_map:
                    clazz = clazz_map[class_name]
                    if inspect.isabstract(clazz):
                        continue
                out.write(f"\n{parent_name}.{class_name} = class {{\n".encode(cls.ENCODING))
                for api in [api for api in WebApplication._all_methods if (api.qualname.split('.')[0] == class_name
                                                                           and api.clazz is not None)]:
                    if api.is_constructor:
                        if isinstance(api.clazz, tuple):
                            module_name, class_name = api.clazz
                            module = sys.modules.get(module_name)
                            api._clazz = getattr(module, class_name)
                        clazz_map[api.clazz.__name__] = api.clazz
                if class_name in clazz_map:
                    clazz = clazz_map[class_name]
                    cls._generate_request(out, route=f"/{class_name}/_create",
                                          api=API(clazz, clazz._create, method=RestMethod.GET,
                                                  content_type="text/plain",
                                                  is_instance_method=False,
                                                  is_constructor=True,
                                                  is_class_method=False,
                                                  expire_on_exit=False),
                                          tab=tab)
                    cls._generate_request(out,
                                          route=f"/{class_name}/expire",
                                          api=API(clazz, clazz._expire, method=RestMethod.GET,
                                                  content_type="text/plain",
                                                  is_instance_method=True,
                                                  is_class_method=False,
                                                  is_constructor=False,
                                                  expire_on_exit=False),
                                          tab=tab)
                tab += "   "
                for method, route_, api in routes:
                    if api.name in ('_create', '_expire'):
                        continue
                    cls._generate_request(out, route_, api, tab)
                tab = tab[:-3]
                out.write("};\n".encode(cls.ENCODING))  # for class end

        ni = '\n'
        out.write(f"{ni}{cls.HYDRA_NANO_SERVICES_CORE};{ni}".encode(cls.ENCODING))
        for name, namespace in namespaces.child_namespaces.items():
            if name.startswith('hydra.nano_services'):
                continue
            name = "hydra.nano_services." + name
            out.write(f"{name} = class {{}};\n".encode(cls.ENCODING))
            tab += "   "
            process_namespace(namespace, name)

    @classmethod
    def _generate_request(cls, out: IO, route: str, api: API, tab: str):
        try:
            offset = 1 if 'self' in api._func.__code__.co_varnames else 0
        except:
            offset = 0
        docs = APIDoc()
        try:
            api_doc = docs.generate(api=api,
                                    flavor=APIDoc.Flavor.JAVASCRIPT, indent=tab)
        except Exception as e:
            api_doc = f"/**\n<<Unable to generate>> {e}\n**/\n"
        out.write(b'\n')
        out.write(api_doc.encode('utf-8'))
        argnames = [k for k in api.arg_annotations.keys() if k not in [api._vararg, api._varkwds]]
        if api.is_constructor:
            if api.name == '_create':
                out.write(
                    f"{tab}constructor({', '.join(argnames)}) {{\n".encode(
                        cls.ENCODING))
            else:
                out.write(
                    f"{tab}static async {api.name}({', '.join(argnames)}) {{\n".encode(
                        cls.ENCODING))
        else:
            name = api.name if not api.name.startswith('_') else api.name[1:]
            static_text = "" if api.is_instance_method else "static "
            out.write(f"{tab}{static_text}async{'*' if api.has_streamed_response else ''} {name}("
                      f"{', '.join(argnames)}) {{\n".
                      encode(cls.ENCODING))
        if api.async_arg_annotations:
            streamed_param = list(api.async_arg_annotations.keys())[0]
            argnames.remove(streamed_param)
        else:
            streamed_param = None
        if hasattr(api.return_type, '__dataclass_fields__'):
            convert = "convert_complex"
        elif str(api.return_type).startswith('typing.Dict') or str(api.return_type).startswith('typing.List'):
            convert = "convert_complex"
        else:
            convert = {str: "convert_str",
                       int: "convert_int",
                       datetime.datetime: "convert_datetime",
                       float: "convert_float",
                       uuid.UUID: "convert_str",
                       bool: "convert_bool",
                       bytes: "convert_bytes",
                       dict: "convert_complex",
                       list: "convert_complex",
                       tuple: "convert_complex",
                       set: "convert_complex",
                       None: "convert_None"}.get(api.return_type, "convert_complex")
        tab += "   "
        self_param_code = f"{tab}  \"self\": this.self_id{',' if argnames else ''}" + '\n' if offset == 1 else ""
        param_code = ',\n'.join([f"{tab}   \"{argname}\": {argname}" for argname in argnames])
        param_code = f"""
{tab}let params = {{
{self_param_code}{param_code}
{tab}}}"""
        out.write(param_code.encode('utf-8'))
        if api.name == '_create':
            assert api.is_constructor
        if api.has_streamed_response:
            out.write(f"""
{tab}for await (var chunk of hydra.nano_services.fetch_{api.method.value}_streamed("{route}",
{tab}                    "{api.content_type}",
{tab}                    params,
{tab}                    hydra.nano_services.{convert},
{tab}                    {str(api.return_type == bytes).lower()}
{tab}                    {f", {streamed_param}" if streamed_param else ""})){{
{tab}   yield chunk;
{tab}}}
{tab[:-3]}}}
""".encode('utf-8'))
        elif api.is_constructor:
            if api.name == '_create':
                out.write(f"""
{tab}if (arguments[0] instanceof HYDRA_NANO_SERVICES_UUID){{
{tab}    this.self_id = arguments[0].uuid;
{tab}}} else {{
{tab}    let request = new XMLHttpRequest();
{tab}    request.open("GET","{route}" + hydra.nano_services.compute_query(params), false);
{tab}    request.setRequestHeader('Content-Type', "{api.content_type}");
{tab}    request.send(null);
{tab}    if (request.status === 200){{
{tab}           this.self_id = request.responseText;
{tab}    }} else {{alert(request.responseText)
{tab}          throw request.stats;
{tab}    }}
{tab}}}
{tab[:-3]}}}
    """.encode('utf-8'))
            else:
                if hasattr(api.clazz, 'jsonrepr'):
                    response_text = f"JSON.parse(request.responseText)[\"{api.uuid_param}\"]"
                else:
                    response_text = "request.responseText"
                out.write(f"""
{tab}let request = new XMLHttpRequest();
{tab}request.open("GET","{route}" + hydra.nano_services.compute_query(params), false);
{tab}request.setRequestHeader('Content-Type', "{api.content_type}");
{tab}request.send(null);
{tab}let self_id
{tab}if (request.status === 200){{
{tab}    self_id = {response_text};
{tab}}} else {{alert(request.responseText )
{tab}    throw request.stats;
{tab}}}
""".encode('utf-8'))
                out.write(f"""
{tab}return new {api.qualname.split('.')[0]}(new HYDRA_NANO_SERVICES_UUID(self_id));
{tab[:-3]}}}
                """.encode('utf-8'))
        else:
            out.write(f"""
{tab}return await hydra.nano_services.fetch_{api.method.value}("{route}", "{api.content_type}", params,
{tab}           hydra.nano_services.{convert} {f", {streamed_param}" if streamed_param else ""});
{tab[:-3]}}}
""".encode('utf-8'))
