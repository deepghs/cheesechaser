"""
This module provides functionality for querying Danbooru image board using its API.

It includes a class `DanbooruIdQuery` which extends `BaseWebQuery` to perform
tag-based searches on Danbooru, handling authentication, pagination, and result filtering.
"""

import logging
from typing import List, Optional, Union, Callable
from urllib.parse import urljoin

import httpx
import requests

from .base import BaseWebQuery
from ..utils import get_requests_session, srequest


class DanbooruIdQuery(BaseWebQuery):
    """
    A class for querying Danbooru image board using tags.

    This class extends BaseWebQuery to provide specific functionality for
    interacting with the Danbooru API. It allows searching for posts using tags,
    handles authentication, and provides methods for pagination and result filtering.

    :param tags: A list of tags to search for on Danbooru.
    :type tags: List[str]
    :param filters: Optional list of filter functions to apply to the query results.
    :type filters: Optional[List[Callable[[dict], bool]]]
    :param username: Optional username for Danbooru API authentication.
    :type username: Optional[str]
    :param api_key: Optional API key for Danbooru API authentication.
    :type api_key: Optional[str]
    :param site_url: The base URL of the Danbooru site, defaults to 'https://danbooru.donmai.us'.
    :type site_url: str
    """

    def __init__(self, tags: List[str], filters: Optional[List[Callable[[dict], bool]]] = None,
                 username: Optional[str] = None, api_key: Optional[str] = None,
                 site_url: str = 'https://danbooru.donmai.us'):
        BaseWebQuery.__init__(self, filters=filters)
        if username and api_key:
            self.auth = (username, api_key)
        else:
            self.auth = None
        self.site_url = site_url
        self.tags = tags

    def _get_session(self) -> Union[httpx.Client, requests.Session]:
        """
        Initialize and return a session for Danbooru API requests.

        This method attempts to create a session with appropriate headers and
        verifies it by making a test request to the Danbooru API.

        :return: An authenticated session for making requests to the Danbooru API.
        :rtype: Union[httpx.Client, requests.Session]
        """
        while True:
            session = get_requests_session(use_httpx=True)
            session.headers.update({
                'Content-Type': 'application/json; charset=utf-8',
            })

            self._try_acquire_api_access()
            logging.info(f'Try initializing session for danbooru API, '
                         f'user agent: {session.headers["User-Agent"]!r}.')
            resp = srequest(session, 'GET', f'{self.site_url}/posts.json', params={
                "format": "json",
                "tags": '1girl',
            }, auth=self.auth, raise_for_status=False)
            if resp.status_code // 100 == 2:
                return session

    def _get_length(self) -> Optional[int]:
        """
        Get the total number of posts matching the query tags.

        :return: The total number of posts matching the query tags, or None if unavailable.
        :rtype: Optional[int]
        """
        self._try_acquire_api_access()
        resp = srequest(self.session, 'GET', urljoin(self.site_url, '/counts/posts.json'), params={
            'tags': ' '.join(self.tags),
        }, auth=self.auth)
        return resp.json()['counts']['posts']

    def _iter_items(self):
        """
        Iterate over all posts matching the query tags.

        This method handles pagination, making multiple requests to the Danbooru API
        as needed to retrieve all matching posts.

        :yield: Dictionary containing information about each matching post.
        """
        page = 1
        page_size: int = 200
        while True:
            self._try_acquire_api_access()
            logging.info(f'Query danbooru API for {self.tags!r}, page: {page!r}.')
            resp = srequest(self.session, 'GET', f'{self.site_url}/posts.json', params={
                "format": "json",
                "limit": str(page_size),
                "page": str(page),
                "tags": ' '.join(self.tags),
            }, auth=self.auth)
            resp.raise_for_status()
            if not resp.json():
                break

            yield from resp.json()
            page += 1
            if page > 200000 // page_size:
                break

    def __repr__(self):
        """
        Return a string representation of the DanbooruIdQuery instance.

        :return: A string representation of the instance.
        :rtype: str
        """
        return f'<{self.__class__.__name__} tags: {self.tags!r}>'
