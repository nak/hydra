import asyncio
from abc import abstractmethod
from typing import Optional, Dict

from hydra.nano_services.api import RestMethod
from hydra.nano_services.client import WebInterface
from hydra.nano_services.decorators import web_api


class RestAPIExampleErrorAsyncInterface(WebInterface):
    """
    HTTP resource for testing ReST examples, with all static methods (interface definition)
    """

    @classmethod
    @web_api(content_type='text/plain', method=RestMethod.GET)
    @abstractmethod
    async def api_get_basic(cls, param1: int, param2: bool, param3: float, param4: str = "text",
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
    async def api_post_basic(cls, param1: int, param2: bool, param3: float, param4: Optional[str] = "text") -> str:
        """
        Some sort of doc
        :param param1:
        :param param2:
        :param param3:
        :param param4:
        :return: stream of int
        """


class RestAPIExampleErrorAsync(RestAPIExampleErrorAsyncInterface):
    """
    HTTP resource for testing ReST examples, with all static methods
    """
    result_queue: Optional[asyncio.Queue] = None

    def __init__(self, val: int):
        self._val = val

    @classmethod
    @web_api(content_type='text/plain', method=RestMethod.GET)
    async def api_get_basic(cls, param1: int, param2: bool, param3: float, param4: str = "text",
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
    @web_api(content_type='text/json', method=RestMethod.GET)
    async def api_post_basic(cls, param1: int, param2: bool, param3: float, param4: Optional[str] = "text") -> str:
        """
        Some sort of doc
        :param param1:
        :param param2:
        :param param3:
        :param param4:
        :return: stream of int
        """
        return "called basic post operation"
