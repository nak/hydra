import asyncio
from abc import abstractmethod
from typing import Optional, Dict, AsyncIterator, List

from hydra.nano_services.api import RestMethod, AsyncLineIterator
from hydra.nano_services.client import WebInterface
from hydra.nano_services.decorators import web_api


class RestAPIExampleAsyncPostInterface(WebInterface):
    """
    HTTP resource for testing ReST examples, with all static methods (interface definition)
    """

    @classmethod
    @web_api(content_type='text/plain', method=RestMethod.POST, is_constructor=True, )
    @abstractmethod
    async def explicit_constructor(cls, val: int) -> "RestAPIExampleAsyncPost":
        """
        constructor
        """

    @classmethod
    @web_api(content_type='text/plain', method=RestMethod.POST)
    @abstractmethod
    async def api_post_basic(cls, *varargs: int, param1: int, param2: bool, param3: float, param4: str = "text",
                            param5: Dict[str, float] = {'f1': 1.0, 'f2': 2.0}) -> str:
        """
        Some sort of doc
        :param param1: docs for first param
        :param param2: docs for 2nd param
        :param param3: docs for param #3
        :param param4: docs for last param
        :param param5: dict value
        :return: String for test_api_basic
        """

    @classmethod
    @web_api(content_type='text/json', method=RestMethod.POST)
    @abstractmethod
    async def api_post_stream(cls, param1: int, param2: bool, param3: float, param4: Optional[str] = None) \
            -> AsyncIterator[int]:
        """
        Some sort of doc
        :param param1:
        :param param2:
        :param param3:
        :param param4:
        :return: stream of int
        """
        yield None  # not called as abstract, but tells Python this is an async generator

    @classmethod
    @web_api(content_type='text/json', method=RestMethod.POST)
    @abstractmethod
    async def api_post_stream_bytes(cls, param1: int, param2: bool, param3: float, param4: Optional[str] = None,
                                   param5: Optional[List[int]] = None) -> AsyncIterator[bytes]:
        """
        Some sort of doc
        :param param1:
        :param param2:
        :param param3:
        :param param4:
        :param param5:
        :return: stream of int
        """

    @classmethod
    @web_api(content_type='text/plain', method=RestMethod.POST)
    @abstractmethod
    async def api_post_streamed_req_and_resp(cls, param1: int, param2: bool, param3: float, param4: AsyncLineIterator)\
            -> AsyncIterator[str]:
        """
        Some sort of doc
        :param param1:
        :param param2:
        :param param3:
        :param param4:
        :return: stream of int
        """

    @classmethod
    @web_api(content_type='text/json', method=RestMethod.POST)
    @abstractmethod
    async def api_post_stream_text(cls, param1: int, param2: bool, param3: float, param4: Optional[str] = None) \
            -> AsyncIterator[str]:
        """
        Some sort of doc
        :param param1:
        :param param2:
        :param param3:
        :param param4:
        :return: stream of int
        """

    @classmethod
    @web_api(content_type='text/plain', method=RestMethod.POST)
    @abstractmethod
    async def publish_result(cls, result: str) -> None:
        """

        """

    @web_api(content_type='text/plain', method=RestMethod.POST)
    @abstractmethod
    async def my_value(self) -> int:
        """
        Return value of this instance
        """

    @web_api(content_type='text/plain', method=RestMethod.POST)
    @abstractmethod
    async def my_value_repeated(self, count: int) -> AsyncIterator[int]:
        """
        Return value of this instance
        """
        yield None

    @web_api(content_type='text/plain', method=RestMethod.POST)
    @abstractmethod
    async def my_value_repeated_string(self, count: int) -> AsyncIterator[str]:
        """
        """
        raise NotImplemented
        yield None


class RestAPIExampleAsyncPost(RestAPIExampleAsyncPostInterface):
    """
    HTTP resource for testing ReST examples, with all static methods
    """
    result_queue: Optional[asyncio.Queue] = None
    disconnected = False

    def __init__(self, val: int):
        self._val = val

    @classmethod
    @web_api(content_type='text/plain', method=RestMethod.POST, is_constructor=True, )
    async def explicit_constructor(cls, val: int) -> "RestAPIExampleAsyncPost":
        return RestAPIExampleAsyncPost(val)

    @classmethod
    @web_api(content_type='text/plain', method=RestMethod.POST)
    async def api_post_basic(cls, *varargs: int, param1: int, param2: bool, param3: float, param4: str = "text",
                            param5: Dict[str, float] = {'f1': 1.0, 'f2': 2.0}) -> str:
        """
        Some sort of doc
        :param param1: docs for first param
        :param param2: docs for 2nd param
        :param param3: docs for param #3
        :param param4: docs for last param
        :param param5:
        :return: String for test_api_basic
        """
        return f"Response to test_api_basic {param5['f1']:.1f} {int(param5['f2'])}"

    @classmethod
    @web_api(content_type='text/json', method=RestMethod.POST)
    async def api_post_stream(cls, param1: int, param2: bool, param3: float, param4: Optional[str] = None) \
            -> AsyncIterator[int]:
        """
        Some sort of doc
        :param param1:
        :param param2:
        :param param3:
        :param param4:
        :return: stream of int
        """
        for index in range(10):
            yield index
            await asyncio.sleep(0.02)

    @classmethod
    @web_api(content_type='text/json', method=RestMethod.POST)
    async def api_post_stream_bytes(cls, param1: int, param2: bool, param3: float, param4: Optional[str] = None,
                                   param5: Optional[List[int]] = None) -> AsyncIterator[bytes]:
        """
        Some sort of doc
        :param param1:
        :param param2:
        :param param3:
        :param param4:
        :param param5:
        :return: stream of int
        """
        for index in range(10):
            yield f"POST COUNT: {index}"
            await asyncio.sleep(0.02)
        yield f"DONE {param5}" if param5 else "DONE"

    @classmethod
    @web_api(content_type='text/plain', method=RestMethod.POST)
    async def api_post_streamed_req_and_resp(cls, param1: int, param2: bool, param3: float, param4: AsyncLineIterator)\
            -> AsyncIterator[str]:
        """
        Some sort of doc
        :param param1:
        :param param2:
        :param param3:
        :param param4:
        :return: stream of int
        """
        async for line in param4:
            yield f"ECHO: {line}"
            await asyncio.sleep(0.02)

    @classmethod
    @web_api(content_type='text/json', method=RestMethod.POST)
    async def api_post_stream_text(cls, param1: int, param2: bool, param3: float, param4: Optional[str] = None) \
            -> AsyncIterator[str]:
        """
        Some sort of doc
        :param param1:
        :param param2:
        :param param3:
        :param param4:
        :return: stream of int
        """
        for index in range(10):
            yield f"COUNT: {index}"
            await asyncio.sleep(0.02)
        yield "DONE"

    @classmethod
    @web_api(content_type='text/plain', method=RestMethod.POST)
    async def publish_result(cls, result: str) -> None:
        await RestAPIExampleAsyncPost.result_queue.put(result)

    @web_api(content_type='text/plain', method=RestMethod.POST)
    async def my_value(self) -> int:
        return self._val

    @web_api(content_type='text/plain', method=RestMethod.POST)
    async def my_value_repeated(self, count: int) -> AsyncIterator[int]:
        for _ in range(count):
            yield self._val

    async def _disconnected(self, item: int):
        assert item == str(self._val) * 65537
        self.__class__.disconnected = True

    @web_api(content_type='text/plain', method=RestMethod.POST, on_disconnect=_disconnected)
    async def my_value_repeated_string(self, count: int) -> AsyncIterator[str]:
        for _ in range(count):
            yield str(self._val)*65537
