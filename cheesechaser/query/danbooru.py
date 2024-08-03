import logging
from typing import List, Optional, Union, Callable
from urllib.parse import urljoin

import httpx
import requests

from .base import _BaseWebQuery
from ..utils import get_requests_session, srequest


class DanbooruIdQuery(_BaseWebQuery):
    def __init__(self, tags: List[str], filters: Optional[List[Callable[[dict], bool]]] = None,
                 username: Optional[str] = None, api_key: Optional[str] = None,
                 site_url: Optional[str] = 'https://danbooru.donmai.us'):
        _BaseWebQuery.__init__(self, filters=filters)
        if username and api_key:
            self.auth = (username, api_key)
        else:
            self.auth = None
        self.site_url = site_url
        self.tags = tags

    def _get_session(self) -> Union[httpx.Client, requests.Session]:
        while True:
            session = get_requests_session(use_httpx=True)
            session.headers.update({
                'Content-Type': 'application/json; charset=utf-8',
            })

            logging.info(f'Try initializing session for danbooru API, '
                         f'user agent: {session.headers["User-Agent"]!r}.')
            resp = srequest(session, 'GET', f'{self.site_url}/posts.json', params={
                "format": "json",
                "tags": '1girl',
            }, auth=self.auth, raise_for_status=False)
            if resp.status_code // 100 == 2:
                return session

    def _get_length(self) -> Optional[int]:
        resp = srequest(self.session, 'GET', urljoin(self.site_url, '/counts/posts.json'), params={
            'tags': ' '.join(self.tags),
        }, auth=self.auth)
        return resp.json()['counts']['posts']

    def _iter_items(self):
        page = 1
        while True:
            logging.info(f'Query danbooru API for {self.tags!r}, page: {page!r}.')
            resp = srequest(self.session, 'GET', f'{self.site_url}/posts.json', params={
                "format": "json",
                "limit": "200",
                "page": str(page),
                "tags": ' '.join(self.tags),
            }, auth=self.auth)
            resp.raise_for_status()
            if not resp.json():
                break

            yield from resp.json()
            page += 1

    def __repr__(self):
        return f'<{self.__class__.__name__} tags: {self.tags!r}>'
