import datetime
import json
import threading
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path, PosixPath
from typing import Dict, List, Union, Tuple, Set, Optional

import pytest
from frozenlist import FrozenList
from frozendict import frozendict

from hydra.nano_services.conversions import to_str, from_str, normalize_from_json

class Pickleable:

    def __init__(self, host: str = None, port: int = None, semaphore = None):
        self.host = host or "localhost"
        self.port = port or 12345
        self.semaphore = semaphore or threading.Semaphore(0)

    def __getstate__(self):
        result = self.__dict__.copy()
        del result['semaphore']
        return result

    def __setstate__(self, state):
        self.host, self.port = state['host'], state['port']
        self.semaphore = threading.Semaphore(0)




class Test:

    def test_to_str_from_pickleable(self):
        assert json.loads(to_str(Pickleable())) == {'host': 'localhost', 'port': 12345}

    def test_to_str_from_tuple(self):
        assert to_str((1, '2')) == json.dumps([1, '2'])

    def test_to_str_from_set(self):
        assert to_str({1, '2'}) in (json.dumps([1, '2']), json.dumps(['2', 1]))

    def test_to_str_from_int(self):
        assert to_str(1343) == "1343"
        assert to_str(-1343) == "-1343"

    def test_to_str_from_float(self):
        assert to_str(-0.345) == "-0.345"

    def test_to_str_from_bool(self):
        assert to_str(True) == "true"
        assert to_str(False) == "false"

    def test_to_str_from_path(self):
        assert to_str(Path("/usr/bin")) == "/usr/bin"
        assert to_str(PosixPath("/usr/bin")) == "/usr/bin"

    def test_to_str_from_list(self):
        assert to_str(['a', 'b', 'c']) == json.dumps(['a', 'b', 'c'])

    def test_to_str_from_frozenlist(self):
        assert to_str(FrozenList(['a', 'b', 'c'])) == json.dumps(['a', 'b', 'c'])

    def test_to_str_from_frozenset(self):
        assert to_str(frozenset(['a', 'b', 'c'])) in (
            json.dumps(['a', 'b', 'c']),
            json.dumps(['a', 'c', 'b']),
            json.dumps(['b', 'a', 'c']),
            json.dumps(['b', 'c', 'a']),
            json.dumps(['c', 'b', 'a']),
            json.dumps(['c', 'a', 'b']),
        )

    def test_to_str_from_dict(self):
        assert to_str({'a': 1, 'b': 3, 'c': 'HERE'}) == json.dumps({'a': 1, 'b': 3, 'c': 'HERE'})

    def test_to_str_from_dataclass(self):
        @dataclass
        class SubData:
            s: str
            l: List[int]

        @dataclass
        class Data:
            f: float
            i: int
            d: Dict[str, str]
            subdata: SubData

        fval = 8883.234
        subd = SubData("name", [1, -5, 34])
        data = Data(fval, 99, {'A': 'a'}, subd)
        assert to_str(data) == json.dumps({
            'f': data.f,
            'i': 99,
            'd': {'A': 'a'},
            'subdata': {'s': "name", 'l': [1, -5, 34]}
        })

    def test_pickleable_from_str(self):
        item = Pickleable()
        text = to_str(item)
        assert json.loads(text) == {'host': 'localhost', 'port': 12345}
        item2 = normalize_from_json(json.loads(text), Pickleable)
        assert item2.host == "localhost"
        assert item2.port == 12345
        assert isinstance(item.semaphore, threading.Semaphore)

    def test_int_from_str(self):
        assert from_str("1234", int) == 1234

    def test_uuid_from_str(self):
        u = uuid.uuid4()
        assert from_str(str(u), uuid.UUID) == u

    def test_optional_int_from_str(self):
        assert from_str("1234", Optional[int]) == 1234
        assert from_str('', Optional[int]) == None
        assert from_str('', int | None) == None

    def test_datetime_from_str(self):
        d = datetime.datetime.now()
        assert from_str(d.isoformat(), datetime.datetime) == d
        assert normalize_from_json(d.isoformat(), datetime.datetime) == d

    def test_float_from_str(self):
        assert from_str("-9.3345", float) == pytest.approx(-9.3345)
        assert normalize_from_json('9.3345', float) == pytest.approx(9.3345)

    def test_path_from_str(self):
        assert from_str("/usr/bin", Path) == Path("/usr/bin")
        assert from_str("/usr/bin", PosixPath) == Path("/usr/bin")
        assert normalize_from_json("/no/path", PosixPath) == Path("/no/path")
        assert normalize_from_json("/no/path", Path) == Path("/no/path")

    def test_bool_from_str(self):
        assert from_str("TruE", bool) is True
        assert from_str("false", bool) is False
        assert normalize_from_json("true", bool) is True

    def test_list_from_str(self):
        assert from_str("[0, -3834, 3419]", List[int]) == [0, -3834, 3419]
        assert normalize_from_json(['0', '-3834', '3419'], List[int]) == [0, -3834, 3419]
        assert normalize_from_json(['0', '-3834', '3419'], list[int]) == [0, -3834, 3419]

    def test_frozenset_from_str(self):
        assert from_str("[0, -3834, 3419]", frozenset[int]) == frozenset([0, -3834, 3419])
        assert normalize_from_json(['0', '-3834', '3419'], frozenset[int]) == frozenset([0, -3834, 3419])

    def test_frozenlist_from_str(self):
        assert from_str("[0, -3834, 3419]", FrozenList[int]) == FrozenList([0, -3834, 3419])
        assert normalize_from_json(['0', '-3834', '3419'], FrozenList[int]) == FrozenList([0, -3834, 3419])

    def test_set_from_str(self):
        assert from_str("[0, -3834, 3419]", Set[int]) == {0, -3834, 3419}
        assert normalize_from_json(['0', '-3834', '3419'], Set[int]) == {0, -3834, 3419}

    def test_tuple_from_str(self):
        assert from_str("[0, -3834, \"hello\"]", Tuple[int, int, str]) == (0, -3834, "hello")
        assert normalize_from_json(['0', '-3834', "hello"], Tuple[int, int, str]) == (0, -3834, "hello")

    def test_dict_from_str(self):
        d = {'name': 'Jane', 'val': 34}
        assert from_str(json.dumps(d), Dict[str, Union[str, int]]) == d
        assert normalize_from_json(d, Dict[str, Union[str, int]]) == d
        assert normalize_from_json(d, dict[str, Union[str, int]]) == d
        assert from_str(json.dumps(d), Dict[str, str | int]) == d
        assert normalize_from_json(d, Dict[str, str | int]) == d
        assert normalize_from_json(d, dict[str, str | int]) == d

    def test_ordered_dict_from_str(self):
        items = [('name', 'Jane'), ('val', 34)]
        d = OrderedDict(items)
        assert from_str(json.dumps([(k, v) for k,v in d.items()]), OrderedDict[str, Union[str, int]]) == d
        assert normalize_from_json(items, OrderedDict[str, Union[str, int]]) == d
        assert from_str(json.dumps([(k, v) for k,v in d.items()]), OrderedDict[str, str | int]) == d
        assert normalize_from_json(items, OrderedDict[str, str | int]) == d

    def test_dict_from_str_complex_keys(self):
        d = {frozenset(['a', 'b']): 34, frozenset(['c']): 'Jane'}
        s = to_str(d)
        assert from_str(s, dict[frozenset[str], str | int]) == d

    def test_frozendict_from_str(self):
        d = {'name': 'Jane', 'val': 34}
        assert from_str(json.dumps(d), frozendict[str, Union[str, int]]) == frozendict(d)
        assert normalize_from_json(d, frozendict[str, Union[str, int]]) == frozendict(d)
        assert from_str(json.dumps(d), frozendict[str, str | int]) ==  frozendict(d)
        assert normalize_from_json(d, frozendict[str, str | int]) ==  frozendict(d)

    def test_dataclass_from_str(self):

        @dataclass
        class SubData:
            s: str
            l: List[int]

        @dataclass
        class Data:
            f: float
            i: int
            d: Dict[str, str]
            subdata: SubData

        d = {
            'f': 0.909222,
            'i': 99,
            'd': {'A': 'a'},
            'subdata': {'s': "name", 'l': [1, -5, 34]}
        }
        raw_data = d.copy()
        image = json.dumps(d)
        d['subdata'] = SubData(**d['subdata'])
        assert from_str(image, Data) == Data(**d)
        assert normalize_from_json(raw_data, Data) == Data(**d)
