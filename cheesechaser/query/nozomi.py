"""
This module provides functionality for querying and iterating over Nozomi IDs.

It includes classes and functions for interacting with the Nozomi API, handling tag-based queries,
and managing the retrieval of Nozomi IDs. The module supports both positive and negative tag filtering,
as well as ordering options for the retrieved IDs.

Key components:

- NozomiIdIterator: A class for iterating over Nozomi IDs based on tags and ordering.
- iter_nozomi_ids: A function that combines multiple NozomiIdIterators to filter IDs based on tags.
- NozomiIdQuery: A class that extends BaseWebQuery to provide a query interface for Nozomi IDs.

This module is useful for applications that need to interact with the Nozomi API and process
large sets of Nozomi IDs efficiently.
"""

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
    """
    Decode Nozomi-encoded data into integers.

    This function takes a bytes-like object and yields integers decoded from it.
    Each integer is constructed from 4 bytes in big-endian order.

    :param n: The Nozomi-encoded data to decode.
    :type n: bytes-like object
    :return: An iterator of decoded integers.
    :rtype: Iterator[int]
    """
    for i in range(0, len(n), 4):
        yield (n[i] << 24) + (n[i + 1] << 16) + (n[i + 2] << 8) + n[i + 3]


_NOT_SET = object()


class NozomiIdIterator:
    """
    An iterator class for Nozomi IDs based on tags and ordering.

    This class handles the retrieval and iteration of Nozomi IDs from the Nozomi API.
    It supports filtering by tags and ordering the results.

    :param tag: The tag to filter the Nozomi IDs (optional).
    :type tag: Optional[str]
    :param order_by: The ordering method for the IDs ('popular' or 'date').
    :type order_by: OrderByTyping
    :param session: A custom session object for making HTTP requests (optional).
    :type session: requests.Session, optional
    """

    def __init__(self, tag: Optional[str] = None, order_by: OrderByTyping = 'date', session=None):
        self.tag: Optional[str] = tag
        self.order_by = order_by
        self._session = session or get_requests_session()
        self._resp: Optional[Response] = None
        self._length = _NOT_SET
        self._lock = Lock()

    def _get_target_url(self) -> str:
        """
        Construct the target URL for the Nozomi API request.

        :return: The constructed URL for the API request.
        :rtype: str
        """
        if not self.tag:
            if self.order_by == 'popular':
                return 'https://n.nozomi.la/index-Popular.nozomi'
            else:
                return 'https://n.nozomi.la/index.nozomi'
        else:
            self.tag: str
            if self.order_by == 'popular':
                return f'https://j.nozomi.la/nozomi/popular/{quote_plus(self.tag)}-Popular.nozomi'
            else:
                return f'https://j.nozomi.la/nozomi/{quote_plus(self.tag)}.nozomi'

    def _make_request(self):
        """
        Make the HTTP request to the Nozomi API and process the response.

        This method is responsible for initiating the API request and handling the response,
        including setting the length of the iterator if possible.
        """
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
        """
        Iterate over the Nozomi IDs.

        This method yields the Nozomi IDs retrieved from the API, decoding them as needed.

        :return: An iterator of Nozomi IDs.
        :rtype: Iterator[int]
        """
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
        """
        Close the HTTP response if it exists.
        """
        if self._resp:
            return self._resp.close()

    def close(self):
        """
        Safely close the iterator and its associated HTTP response.
        """
        with self._lock:
            self._close()

    def __len__(self):
        """
        Get the length of the iterator.

        :return: The number of Nozomi IDs in the iterator, or None if unknown.
        :rtype: int or None
        """
        with self._lock:
            self._make_request()
            return self._length


def iter_nozomi_ids(tags: List[str], negative_tags: Optional[List[str]] = None,
                    order_by: OrderByTyping = 'date', session=None) -> Iterator[int]:
    """
    Iterate over Nozomi IDs based on given tags and negative tags.

    This function combines multiple NozomiIdIterators to filter Nozomi IDs based on
    the provided tags and negative tags. It supports ordering the results.

    :param tags: A list of tags to filter the Nozomi IDs.
    :type tags: List[str]
    :param negative_tags: A list of tags to exclude from the results (optional).
    :type negative_tags: Optional[List[str]]
    :param order_by: The ordering method for the IDs ('popular' or 'date').
    :type order_by: OrderByTyping
    :param session: A custom session object for making HTTP requests (optional).
    :type session: requests.Session, optional
    :return: An iterator of filtered Nozomi IDs.
    :rtype: Iterator[int]
    """
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
    """
    A query class for retrieving Nozomi IDs based on tags and ordering.

    This class extends BaseWebQuery to provide a query interface for Nozomi IDs.
    It supports filtering by tags, negative tags, and ordering the results.

    :param tags: A list of tags to filter the Nozomi IDs.
    :type tags: List[str]
    :param negative_tags: A list of tags to exclude from the results (optional).
    :type negative_tags: List[str]
    :param order_by: The ordering method for the IDs ('popular' or 'date').
    :type order_by: OrderByTyping
    """

    def __init__(self, tags: List[str], negative_tags: Optional[List[str]] = None, order_by: OrderByTyping = 'date'):
        BaseWebQuery.__init__(self, filters=[])
        self.tags = tags
        self.negative_tags = list(negative_tags or [])
        self.order_by = order_by

    def _get_session(self) -> Union[httpx.Client, requests.Session]:
        """
        Get the session object for making HTTP requests.

        :return: A session object for HTTP requests.
        :rtype: Union[httpx.Client, requests.Session]
        """
        return get_requests_session()

    def _iter_items(self) -> Iterator[Any]:
        """
        Iterate over the Nozomi IDs based on the query parameters.

        :return: An iterator of Nozomi IDs.
        :rtype: Iterator[Any]
        """
        yield from iter_nozomi_ids(
            tags=self.tags,
            negative_tags=self.negative_tags,
            order_by=self.order_by,
        )

    def _get_id_from_item(self, item) -> int:
        """
        Extract the ID from an item in the iterator.

        :param item: The item from the iterator.
        :type item: Any
        :return: The Nozomi ID.
        :rtype: int
        """
        return item

    def _get_length(self) -> Optional[int]:
        """
        Get the length of the query results.

        :return: The number of items in the query results, or None if unknown.
        :rtype: Optional[int]
        """
        return None
