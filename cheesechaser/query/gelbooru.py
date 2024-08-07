"""
This module provides functionality for querying Gelbooru image board using its API.

It includes a class for performing searches based on tags and applying filters to the results.
The module utilizes HTTP requests to interact with the Gelbooru API and handles pagination
for retrieving multiple pages of results.
"""

import logging
from typing import Optional, Iterator, Any, Union, List, Callable

import httpx
import requests

from .base import BaseWebQuery
from ..utils import get_requests_session, srequest


class GelbooruIdQuery(BaseWebQuery):
    """
    A class for querying Gelbooru image board and retrieving post information based on tags.

    This class extends BaseWebQuery and implements methods for interacting with the Gelbooru API,
    including session initialization, making API requests, and iterating through search results.

    :param tags: A list of tags to search for on Gelbooru.
    :type tags: List[str]
    :param filters: Optional list of filter functions to apply to the search results.
    :type filters: Optional[List[Callable[[dict], bool]]]
    :param site_url: The base URL of the Gelbooru site, defaults to 'https://gelbooru.com'.
    :type site_url: str

    :ivar tags: The list of tags used for the search.
    :ivar site_url: The base URL of the Gelbooru site.
    :ivar _length: Cached total count of search results.
    """

    def __init__(self, tags: List[str], filters: Optional[List[Callable[[dict], bool]]] = None,
                 site_url: str = 'https://gelbooru.com'):
        BaseWebQuery.__init__(self, filters=filters)
        self.tags = tags
        self.site_url = site_url
        self._length = None

    def _get_session(self) -> Union[httpx.Client, requests.Session]:
        """
        Initialize and return a session for making requests to the Gelbooru API.

        This method attempts to create a session with a valid user agent until successful.

        :return: An initialized session object.
        :rtype: Union[httpx.Client, requests.Session]
        """
        while True:
            session = get_requests_session(use_httpx=False)

            self._try_acquire_api_access()
            logging.info(f'Try initializing session for danbooru API, '
                         f'user agent: {session.headers["User-Agent"]!r}.')
            resp = srequest(session, 'GET', f'{self.site_url}/index.php', params={
                'page': 'dapi',
                's': 'post',
                'q': 'index',
                'tags': ' '.join(self.tags),
                'json': '1',
                'limit': '100',
                'pid': str(0),
            }, raise_for_status=False)
            if resp.status_code // 100 == 2:
                return session

    def _request(self, page: int, page_size: int = 100):
        """
        Make a request to the Gelbooru API for a specific page of results.

        :param page: The page number to request.
        :type page: int
        :return: A tuple containing the response attributes and the list of posts.
        :rtype: Tuple[dict, List[dict]]
        """
        self._try_acquire_api_access()
        logging.info(f'Query gelbooru API for {self.tags!r}, page: {page!r}.')
        resp = srequest(self.session, 'GET', f'{self.site_url}/index.php', params={
            'page': 'dapi',
            's': 'post',
            'q': 'index',
            'tags': ' '.join(self.tags),
            'json': '1',
            'limit': str(page_size),
            'pid': str(page),
        })
        resp.raise_for_status()
        if 'post' in resp.json():
            posts = resp.json()['post']
        else:
            posts = []
        return resp.json()['@attributes'], posts

    def _iter_items(self) -> Iterator[Any]:
        """
        Iterate through all posts matching the search criteria.

        This method handles pagination and yields individual post data.

        :return: An iterator of post data.
        :rtype: Iterator[Any]
        """
        page = 0
        while True:
            _, posts = self._request(page, page_size=100)
            if not posts:
                break

            yield from posts
            page += 1
            if page > 20000 // 100:
                break

    def _get_length(self) -> Optional[int]:
        """
        Get the total number of posts matching the search criteria.

        This method caches the result to avoid unnecessary API calls.

        :return: The total number of matching posts, or None if not available.
        :rtype: Optional[int]
        """
        if self._length is None:
            attrs, _ = self._request(0)
            self._length = attrs['count']
        return self._length

    def __repr__(self):
        """
        Return a string representation of the GelbooruIdQuery instance.

        :return: A string representation of the object.
        :rtype: str
        """
        return f'<{self.__class__.__name__} tags: {self.tags!r}>'
