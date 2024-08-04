"""
This module provides a base class for web querying operations.

It includes functionality for filtering items, managing sessions, and iterating over query results.
The module is designed to be extended for specific web querying tasks.
"""

from typing import Union, Iterator, Optional, Callable, List, Any

import httpx
import requests
from tqdm import tqdm

_LENGTH_NOT_SET = object()
_ItemFilterTyping = Callable[[Any], bool]


class _BaseWebQuery:
    """
    A base class for web querying operations.

    This class provides a foundation for creating web query objects with filtering capabilities.
    It manages sessions (either httpx.Client or requests.Session) and provides methods for
    iterating over query results with optional filtering.

    :param filters: A list of callable filter functions to apply to query results.
    :type filters: Optional[List[_ItemFilterTyping]]
    """

    def __init__(self, filters: Optional[List[_ItemFilterTyping]] = None):
        """
        Initialize the _BaseWebQuery object.

        :param filters: A list of callable filter functions to apply to query results.
        :type filters: Optional[List[_ItemFilterTyping]]
        """
        self._session: Optional[Union[httpx.Client, requests.Session]] = None
        self._filters = list(filters or [])

    def _get_session(self) -> Union[httpx.Client, requests.Session]:
        """
        Get the session object for making HTTP requests.

        This method should be implemented by subclasses to return either an httpx.Client
        or a requests.Session object.

        :return: A session object for making HTTP requests.
        :rtype: Union[httpx.Client, requests.Session]
        :raises NotImplementedError: If not implemented in a subclass.
        """
        raise NotImplementedError  # pragma: no cover

    def _iter_items(self) -> Iterator[Any]:
        """
        Iterate over query result items.

        This method should be implemented by subclasses to yield individual items
        from the query results.

        :return: An iterator of query result items.
        :rtype: Iterator[Any]
        :raises NotImplementedError: If not implemented in a subclass.
        """
        raise NotImplementedError  # pragma: no cover

    def _get_length(self) -> Optional[int]:
        """
        Get the total number of items in the query results.

        This method should be implemented by subclasses to return the total count
        of items, if available.

        :return: The total number of items, or None if not available.
        :rtype: Optional[int]
        :raises NotImplementedError: If not implemented in a subclass.
        """
        raise NotImplementedError  # pragma: no cover

    def _get_id_from_item(self, item) -> int:
        """
        Extract the ID from a query result item.

        :param item: A query result item.
        :type item: Any
        :return: The ID of the item.
        :rtype: int
        """
        return item['id']

    def _fn_check(self, item) -> bool:
        """
        Apply all filter functions to an item.

        :param item: A query result item to check against all filters.
        :type item: Any
        :return: True if the item passes all filters, False otherwise.
        :rtype: bool
        """
        for fn in self._filters:
            if not fn(item):
                return False
        return True

    @property
    def session(self) -> Union[httpx.Client, requests.Session]:
        """
        Get the session object, creating it if it doesn't exist.

        :return: The session object for making HTTP requests.
        :rtype: Union[httpx.Client, requests.Session]
        """
        if self._session is None:
            self._session = self._get_session()
        return self._session

    def __iter__(self) -> Iterator[int]:
        """
        Iterate over filtered query result IDs.

        This method yields unique IDs of items that pass all filters.
        It uses tqdm to display a progress bar during iteration.

        :return: An iterator of unique item IDs that pass all filters.
        :rtype: Iterator[int]
        """
        _exist_ids = set()
        for item in tqdm(self._iter_items(), total=self._get_length()):
            id_ = self._get_id_from_item(item)
            if id_ not in _exist_ids and self._fn_check(item):
                yield id_
                _exist_ids.add(id_)
