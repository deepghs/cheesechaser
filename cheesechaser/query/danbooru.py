import copy
import logging
import re
from typing import List, Optional, Iterator
from urllib.parse import urljoin

from cheesechaser.utils import get_requests_session, srequest


class _DanbooruQueryable:
    def include_tags(self, tags: List[str]):
        raise NotImplementedError

    def exclude_tags(self, tags: List[str]):
        raise NotImplementedError

    def min_width(self, min_width: int):
        raise NotImplementedError

    def min_height(self, min_height: int):
        raise NotImplementedError

    def limit(self, count):
        raise NotImplementedError


class DanbooruIdQuery(_DanbooruQueryable):
    def __init__(self, tags: List[str], username: Optional[str] = None, api_key: Optional[str] = None,
                 site_url: Optional[str] = 'https://danbooru.donmai.us'):
        self._session = None

        if username and api_key:
            self.auth = (username, api_key)
        else:
            self.auth = None
        self.site_url = site_url
        self.tags = tags
        self._length = None

    @property
    def session(self):
        if self._session is None:
            while True:
                self._session = get_requests_session(use_httpx=True)
                self._session.headers.update({
                    'Content-Type': 'application/json; charset=utf-8',
                })

                logging.info(f'Try initializing session for danbooru API, '
                             f'user agent: {self._session.headers["User-Agent"]!r}.')
                resp = srequest(self.session, 'GET', f'{self.site_url}/posts.json', params={
                    "format": "json",
                    "tags": '1girl',
                }, auth=self.auth, raise_for_status=False)
                if resp.status_code // 100 == 2:
                    break

        return self._session

    def __len__(self) -> int:
        if self._length is None:
            resp = srequest(self.session, 'GET', urljoin(self.site_url, '/counts/posts.json'), params={
                'tags': ' '.join(self.tags),
            }, auth=self.auth)
            self._length = resp.json()['counts']['posts']

        if self._length is None:
            raise TypeError(f'Object {self!r} uncountable.')
        return self._length

    def iter_items(self):
        page = 1
        exist_ids = set()
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

            for item in resp.json():
                if item['id'] not in exist_ids:
                    yield item
                    exist_ids.add(item['id'])
            page += 1

    def __iter__(self) -> Iterator[int]:
        for item in self.iter_items():
            yield item['id']

    def __repr__(self):
        return f'<{self.__class__.__name__} tags: {self.tags!r}>'

    def include_tags(self, tags: List[str]):
        return DanbooruIdQueryProxy(self, must_include_tags=tags)

    def exclude_tags(self, tags: List[str]):
        return DanbooruIdQueryProxy(self, must_exclude_tags=tags)

    def min_width(self, min_width: int):
        return DanbooruIdQueryProxy(self, min_width=min_width)

    def min_height(self, min_height: int):
        return DanbooruIdQueryProxy(self, min_height=min_height)

    def limit(self, count):
        return DanbooruIdQueryProxy(self, max_limit=count)


class DanbooruIdQueryProxy(_DanbooruQueryable):
    def __init__(self, query: DanbooruIdQuery, max_limit: int = None,
                 must_include_tags: List[str] = None, must_exclude_tags: List[str] = None,
                 min_width: Optional[int] = None, min_height: Optional[int] = None):
        self._query = query
        self._max_limit = max_limit
        self._must_include_tags = list(must_include_tags or [])
        self._must_exclude_tags = list(must_exclude_tags or [])
        self._min_width = min_width
        self._min_height = min_height

    def iter_items(self):
        count = 0
        for item in self._query.iter_items():
            tags = set(re.split(r'\s+', item["tag_string"]))
            if any(itag not in tags for itag in self._must_include_tags):
                continue
            if any(etag in tags for etag in self._must_exclude_tags):
                continue
            if self._min_width is not None and item['image_width'] < self._min_width:
                continue
            if self._min_height is not None and item['image_height'] < self._min_height:
                continue

            yield item
            count += 1
            if self._max_limit is not None and count >= self._max_limit:
                break

    def __iter__(self):
        for item in self.iter_items():
            yield item['id']

    def _my_params(self):
        return {
            'max_limit': self._max_limit,
            'must_include_tags': self._must_include_tags,
            'must_exclude_tags': self._must_exclude_tags,
            'min_width': self._min_width,
            'min_height': self._min_height,
        }

    def _create_proxy_with_attached_args(self, **kwargs):
        params = copy.deepcopy(self._my_params())
        params.update(copy.deepcopy(kwargs))
        return DanbooruIdQueryProxy(self._query, **params)

    def include_tags(self, tags: List[str]):
        return self._create_proxy_with_attached_args(
            must_include_tags=[*self._must_include_tags, *tags],
        )

    def exclude_tags(self, tags: List[str]):
        return self._create_proxy_with_attached_args(
            must_exclude_tags=[*self._must_exclude_tags, *tags],
        )

    def min_width(self, min_width: int):
        return self._create_proxy_with_attached_args(
            min_width=min_width,
        )

    def min_height(self, min_height: int):
        return self._create_proxy_with_attached_args(
            min_height=min_height,
        )

    def limit(self, count):
        return self._create_proxy_with_attached_args(
            max_limit=count,
        )
