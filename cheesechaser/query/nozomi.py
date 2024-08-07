from threading import Lock
from typing import Optional, Literal, Iterator, List, Any, Union
from urllib.parse import quote_plus

import httpx
import requests
from requests import Response
from tqdm import tqdm

from .base import BaseWebQuery
from ..utils import get_requests_session

OrderByTyping = Literal['popular', 'date']


def _decode_nozomi(n) -> Iterator[int]:
    for i in range(0, len(n), 4):
        yield (n[i] << 24) + (n[i + 1] << 16) + (n[i + 2] << 8) + n[i + 3]


_NOT_SET = object()


class NozomiIdIterator:
    def __init__(self, tag: Optional[str] = None, order_by: OrderByTyping = 'date', session=None):
        self.tag = tag
        self.order_by = order_by
        self._session = session or get_requests_session()
        self._resp: Optional[Response] = None
        self._length = _NOT_SET
        self._lock = Lock()

    def _get_target_url(self) -> str:
        if not self.tag:
            if self.order_by == 'popular':
                return 'https://n.nozomi.la/index-Popular.nozomi'
            else:
                return 'https://n.nozomi.la/index.nozomi'
        else:
            if self.order_by == 'popular':
                return f'https://j.nozomi.la/nozomi/popular/{quote_plus(self.tag)}-Popular.nozomi'
            else:
                return f'https://j.nozomi.la/nozomi/{quote_plus(self.tag)}.nozomi'

    def _make_request(self):
        if not self._resp:
            self._resp = self._session.get(self._get_target_url(), stream=True)
            if not self._resp.ok and self._resp.status_code == 404:
                self._length = 0
            elif self._resp.ok:
                if 'Content-Length' in self._resp.headers:
                    self._length = int(self._resp.headers['Content-Length']) // 4
                else:
                    self._length = None
            else:
                self._length = None
                self._resp.raise_for_status()

    def __iter__(self) -> Iterator[int]:
        try:
            with self._lock:
                self._make_request()
            if self._resp.ok:
                prev = b''
                for chunk in self._resp.iter_content(chunk_size=1 << 20):
                    chunk = prev + chunk
                    chunk, prev = chunk[:len(chunk) // 4 * 4], chunk[len(chunk) // 4 * 4:]
                    yield from _decode_nozomi(chunk)
                assert not prev, f'Still rest of the stream - {prev!r}.'
        finally:
            with self._lock:
                self._close()

    def _close(self):
        if self._resp:
            return self._resp.close()

    def close(self):
        with self._lock:
            self._close()

    def __len__(self):
        with self._lock:
            self._make_request()
            return self._length


def iter_nozomi_ids(tags: List[str], negative_tags: Optional[List[str]] = None,
                    order_by: OrderByTyping = 'date', session=None) -> Iterator[int]:
    if not tags:
        iters = [NozomiIdIterator(None, order_by=order_by, session=session)]
    else:
        iters = [NozomiIdIterator(tag, order_by=order_by, session=session) for tag in tags]
    neg_iters = [NozomiIdIterator(tag, order_by=order_by, session=session) for tag in list(negative_tags or [])]

    prev_iters, last_iter = iters[:-1], iters[-1]
    id_set = None
    for it in prev_iters:
        id_tag_set = set(tqdm(it, desc=f'Tag {it.tag!r}' if it.tag else 'ALL'))
        if id_set is None:
            id_set = id_tag_set
        else:
            id_set = id_set & id_tag_set

    id_neg_set = None
    for it in neg_iters:
        id_tag_set = set(tqdm(it, desc=f'Negative Tag {it.tag!r}' if it.tag else 'Negative ALL'))
        if id_neg_set is None:
            id_neg_set = id_tag_set
        else:
            id_neg_set = id_neg_set & id_tag_set

    for id_ in tqdm(last_iter, desc=f'Tag {last_iter.tag!r}' if last_iter.tag else 'ALL'):
        if (id_set is None or id_ in id_set) and (id_neg_set is None or id_ not in id_neg_set):
            yield id_


class NozomiIdQuery(BaseWebQuery):
    def __init__(self, tags: List[str], order_by: OrderByTyping = 'date'):
        BaseWebQuery.__init__(self, filters=[])
        self.tags = tags
        self.order_by = order_by

    def _get_session(self) -> Union[httpx.Client, requests.Session]:
        return get_requests_session()

    def _iter_items(self) -> Iterator[Any]:
        yield from iter_nozomi_ids(
            tags=self.tags,
            order_by=self.order_by,
        )

    def _get_id_from_item(self, item) -> int:
        return item

    def _get_length(self) -> Optional[int]:
        return None
