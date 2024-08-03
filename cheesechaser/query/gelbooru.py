import logging
from typing import Optional, Iterator, Any, Union, List, Callable

import httpx
import requests

from .base import _BaseWebQuery
from ..utils import get_requests_session, srequest


class GelbooruIdQuery(_BaseWebQuery):
    def __init__(self, tags: List[str], filters: Optional[List[Callable[[dict], bool]]] = None,
                 site_url: str = 'https://gelbooru.com'):
        _BaseWebQuery.__init__(self, filters=filters)
        self.tags = tags
        self.site_url = site_url
        self._length = None

    def _get_session(self) -> Union[httpx.Client, requests.Session]:
        return get_requests_session()

    def _request(self, page: int):
        logging.info(f'Query gelbooru API for {self.tags!r}, page: {page!r}.')
        resp = srequest(self.session, 'GET', f'{self.site_url}/index.php', params={
            'page': 'dapi',
            's': 'post',
            'q': 'index',
            'tags': ' '.join(self.tags),
            'json': '1',
            'limit': '100',
            'pid': str(page),
        })
        resp.raise_for_status()
        if 'post' in resp.json():
            posts = resp.json()['post']
        else:
            posts = []
        return resp.json()['@attributes'], posts

    def _iter_items(self) -> Iterator[Any]:
        page = 0
        while True:
            _, posts = self._request(page)
            if not posts:
                break

            yield from posts
            page += 1

    def _get_length(self) -> Optional[int]:
        if self._length is None:
            attrs, _ = self._request(0)
            self._length = attrs['count']
        return self._length

    def __repr__(self):
        return f'<{self.__class__.__name__} tags: {self.tags!r}>'
