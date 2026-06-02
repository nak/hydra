from typing import Any, Type, Dict

import pytest

from hydra.nano_services.http import _convert_request_param


class Deserialiazlbe:

    def __init__(self, val: int):
        self._val = val

    @classmethod
    def deserialize(cls, text: str):
        return Deserialiazlbe(int(text))

    def __eq__(self, other):
        return other._val == self._val


class MockStreamResponse:

    def __init__(self, status: int, reason: str, headers: Dict[str, str]):
        self.body = b''
        assert status == 200
        assert reason == "OK"
        assert headers.get('Content-Type') in ['text/plain', 'text-streamed; charset=x-user-defined']
        self._status = status

    async def prepare(self, request):
        pass

    async def write(self, content: bytes):
        self.body += b' ' + content.replace('\n', '')

    async def write_eof(self, *args, **kargs):
        self.body += b'\n'



class MockRequestGet:

    def __init__(self, **kwargs):
        self.query = kwargs

    async def prepare(self, request):
        pass


class MockRequestPost:

    def __init__(self, **kwargs):
        self.can_read_body = True
        self.text_content = str(kwargs).replace("'", '"').encode('utf-8')

    async def read(self):
        return self.text_content


class MockRequestPostRaw:

    def __init__(self, param1: bytes, **kwargs):
        self.can_read_body = True
        self.byte_content = param1
        self.query = kwargs

    async def read(self):
        return self.byte_content


class MockRequestPostStream:

    class StreamReader:

        def __init__(self, content: bytes):
            self._content = content
            self._content_lines = content.splitlines()
            self._is_eof = False

        async def read(self, size: int):
            line = self._content[:size]
            self._content = self._content[size:]
            self._is_eof = len(self._content) == 0
            return line

        async def readline(self):
            if not self._content_lines:
                return None
            line = self._content_lines[0]
            self._content_lines = self._content_lines[1:]
            self._is_eof = len(self._content_lines) == 0
            return line

        def is_eof(self):
            return self._is_eof


    def __init__(self, param1: bytes, **kwargs):
        self.can_read_body = True
        self.content = self.StreamReader(param1)
        self.query = kwargs

    async def read(self):
        return str(self.query).encode('utf-8').replace(b'\'', b'"')


class TestDecoratorUtils:

    @pytest.mark.parametrize("text,typ,value", [("True", bool, True), ("false", bool, False), ("18938", int, 18938),
                                                ("1.2345", float, 1.2345), ("This is text", str, "This is text"),
                                                ])
    def test_convert_request_param(self, text: str, typ: Type, value: Any):
        assert _convert_request_param(text, typ) == value

    @staticmethod
    async def func(param1: int, param2: str) -> str:
        assert param1 == 1
        assert param2 == 'text'
        return "RESULT"
