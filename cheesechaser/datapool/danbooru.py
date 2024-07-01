import fnmatch
import os
from contextlib import contextmanager
from typing import Iterable, ContextManager, Tuple, Any

from hfutils.operate.base import get_hf_fs
from natsort import natsorted

from .base import DataPool, ResourceNotFoundError, IncrementIDDataPool

_DEFAULT_DATA_REPO_ID = 'nyanko7/danbooru2023'
_DEFAULT_IDX_REPO_ID = 'deepghs/danbooru2023_index'


class _BaseDanbooruDataPool(IncrementIDDataPool):
    def __init__(self, data_repo_id: str, data_revision: str = 'main',
                 idx_repo_id: str = None, idx_revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=data_repo_id,
            data_revision=data_revision,
            idx_repo_id=idx_repo_id,
            idx_revision=idx_revision,
        )
        self._update_files = None

    def _get_update_files(self):
        if self._update_files is None:
            hf_fs = get_hf_fs()
            self._update_files = natsorted([
                os.path.relpath(file, f'datasets/{self.data_repo_id}')
                for file in hf_fs.glob(f'datasets/{self.data_repo_id}/updates/**/*.tar')
            ])

        return self._update_files

    def _request_possible_archives(self, resource_id) -> Iterable[str]:
        modulo = f'{resource_id % 1000:03d}'
        return [
            f'original/data-0{modulo}.tar',
            f'recent/data-1{modulo}.tar',
            *[file for file in self._get_update_files() if
              fnmatch.fnmatch(os.path.basename(file), f'*-{resource_id % 10}.tar')]
        ]


class DanbooruDataPool(_BaseDanbooruDataPool):
    def __init__(self, data_revision: str = 'main', idx_revision: str = 'main'):
        _BaseDanbooruDataPool.__init__(
            self,
            data_repo_id=_DEFAULT_DATA_REPO_ID,
            data_revision=data_revision,
            idx_repo_id=_DEFAULT_IDX_REPO_ID,
            idx_revision=idx_revision,
        )


class DanbooruStableDataPool(DanbooruDataPool):
    def __init__(self):
        DanbooruDataPool.__init__(
            self,
            data_revision='81652caef9403712b4112a3fcb5d9b4997424ac3',
            idx_revision='20240319',
        )


_N_REPO_ID = 'deepghs/danbooru_newest'


class _DanbooruNewestPartialDataPool(IncrementIDDataPool):
    def __init__(self, data_revision: str = 'main', idx_revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_N_REPO_ID,
            data_revision=data_revision,
            idx_repo_id=_N_REPO_ID,
            idx_revision=idx_revision,
        )


class DanbooruNewestDataPool(DataPool):
    def __init__(self):
        self._old_pool = DanbooruStableDataPool()
        self._newest_pool = _DanbooruNewestPartialDataPool()

    @contextmanager
    def mock_resource(self, resource_id, resource_info) -> ContextManager[Tuple[str, Any]]:
        pools = [self._old_pool, self._newest_pool]
        found = False
        for pool in pools:
            try:
                with pool.mock_resource(resource_id, resource_info) as (td, info):
                    yield td, info
            except ResourceNotFoundError:
                pass
            else:
                found = True
                break

        if not found:
            raise ResourceNotFoundError(f'Resource {resource_id!r} not found.')


_DEFAULT_WEBP_DATA_REPO_ID = 'KBlueLeaf/danbooru2023-webp-4Mpixel'
_DEFAULT_WEBP_IDX_REPO_ID = 'deepghs/danbooru2023-webp-4Mpixel_index'


class DanbooruWebpDataPool(_BaseDanbooruDataPool):
    def __init__(self, data_revision: str = 'main', idx_revision: str = 'main'):
        _BaseDanbooruDataPool.__init__(
            self,
            data_repo_id=_DEFAULT_WEBP_DATA_REPO_ID,
            data_revision=data_revision,
            idx_repo_id=_DEFAULT_WEBP_IDX_REPO_ID,
            idx_revision=idx_revision,
        )

    def _request_possible_archives(self, resource_id) -> Iterable[str]:
        modulo = f'{resource_id % 1000:03d}'
        return [
            f'images/data-0{modulo}.tar',
            f'images/data-1{modulo}.tar',
            *[file for file in self._get_update_files() if
              fnmatch.fnmatch(os.path.basename(file), f'*-{resource_id % 10}.tar')]
        ]


_N_WEBP_RPEO_ID = 'deepghs/danbooru_newest-webp-4Mpixel'


class _DanbooruNewestPartialWebpDataPool(IncrementIDDataPool):
    def __init__(self, data_revision: str = 'main', idx_revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_N_WEBP_RPEO_ID,
            data_revision=data_revision,
            idx_repo_id=_N_WEBP_RPEO_ID,
            idx_revision=idx_revision,
        )


class DanbooruNewestWebpDataPool(DataPool):
    def __init__(self):
        self._old_pool = DanbooruWebpDataPool()
        self._newest_pool = _DanbooruNewestPartialWebpDataPool()

    @contextmanager
    def mock_resource(self, resource_id, resource_info) -> ContextManager[Tuple[str, Any]]:
        pools = [self._old_pool, self._newest_pool]
        found = False
        for pool in pools:
            try:
                with pool.mock_resource(resource_id, resource_info) as (td, info):
                    yield td, info
            except ResourceNotFoundError:
                pass
            else:
                found = True
                break

        if not found:
            raise ResourceNotFoundError(f'Resource {resource_id!r} not found.')
