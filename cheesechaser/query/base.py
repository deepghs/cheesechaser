from typing import Union, Iterator, Optional, Callable, List, Any

import httpx
import requests
from tqdm import tqdm

_LENGTH_NOT_SET = object()
_ItemFilterTyping = Callable[[Any], bool]


class _BaseWebQuery:
    def __init__(self, filters: Optional[List[_ItemFilterTyping]] = None):
        self._session: Optional[Union[httpx.Client, requests.Session]] = None
        self._filters = list(filters or [])

    def _get_session(self) -> Union[httpx.Client, requests.Session]:
        raise NotImplementedError

    def _iter_items(self) -> Iterator[Any]:
        raise NotImplementedError

    def _get_length(self) -> Optional[int]:
        raise NotImplementedError

    def _get_id_from_item(self, item) -> int:
        return item['id']

    def _fn_check(self, item) -> bool:
        for fn in self._filters:
            if not fn(item):
                return False
        return True

    @property
    def session(self) -> Union[httpx.Client, requests.Session]:
        if self._session is None:
            self._session = self._get_session()
        return self._session

    def __iter__(self) -> Iterator[int]:
        _exist_ids = set()
        for item in tqdm(self._iter_items(), total=self._get_length()):
            id_ = self._get_id_from_item(item)
            if id_ not in _exist_ids and self._fn_check(item):
                yield id_
                _exist_ids.add(id_)
