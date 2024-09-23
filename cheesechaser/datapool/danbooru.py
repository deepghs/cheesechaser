"""
This module provides data pool classes for managing and accessing Danbooru image datasets.

It includes classes for handling both original and WebP versions of Danbooru images,
as well as classes for accessing the newest additions to the dataset. The module
utilizes Hugging Face's file system for data storage and retrieval.

Classes:

* `DanbooruDataPool`: Main class for accessing Danbooru image data.
* `DanbooruStableDataPool`: Class for accessing a stable version of Danbooru data.
* `DanbooruNewestDataPool`: Class for accessing both stable and newest Danbooru data.
* `DanbooruWebpDataPool`: Class for accessing WebP versions of Danbooru images.
* `DanbooruNewestWebpDataPool`: Class for accessing both stable and newest WebP Danbooru data.

The module uses various utility functions and classes from the hfutils package
for interacting with the Hugging Face file system.

.. note::
    Danbooru series datasets are all freely-opened.
"""

import fnmatch
import os
from contextlib import contextmanager
from typing import Iterable, ContextManager, Tuple, Any, Optional

from hfutils.operate import get_hf_fs
from hfutils.utils import parse_hf_fs_path, hf_fs_path
from natsort import natsorted

from .base import DataPool, ResourceNotFoundError, IncrementIDDataPool

_DEFAULT_DATA_REPO_ID = 'nyanko7/danbooru2023'
_DEFAULT_IDX_REPO_ID = 'deepghs/danbooru2023_index'


class _BaseDanbooruDataPool(IncrementIDDataPool):
    """
    Base class for Danbooru data pools.

    This class extends IncrementIDDataPool and provides common functionality
    for Danbooru data access.

    :param data_repo_id: The ID of the data repository.
    :type data_repo_id: str
    :param data_revision: The revision of the data repository to use. Defaults to 'main'.
    :type data_revision: str
    :param idx_repo_id: The ID of the index repository. If None, uses the data_repo_id.
    :type idx_repo_id: str
    :param idx_revision: The revision of the index repository to use. Defaults to 'main'.
    :type idx_revision: str
    :param hf_token: Optional Hugging Face token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, data_repo_id: str, data_revision: str = 'main',
                 idx_repo_id: str = None, idx_revision: str = 'main', hf_token: Optional[str] = None):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=data_repo_id,
            data_revision=data_revision,
            idx_repo_id=idx_repo_id,
            idx_revision=idx_revision,
            hf_token=hf_token,
        )
        self._update_files = None

    def _get_update_files(self):
        """
        Retrieve and cache the list of update files.

        This method fetches the list of update files from the Hugging Face file system
        and caches it for future use.

        :return: A sorted list of update file names.
        :rtype: list
        """
        if self._update_files is None:
            hf_fs = get_hf_fs(hf_token=self._hf_token)
            self._update_files = natsorted([
                parse_hf_fs_path(file).filename
                for file in hf_fs.glob(hf_fs_path(
                    repo_id=self.data_repo_id,
                    repo_type='dataset',
                    filename='updates/**/*.tar',
                    revision=self.data_revision,
                ))
            ])

        return self._update_files

    def _request_possible_archives(self, resource_id) -> Iterable[str]:
        """
        Generate a list of possible archive names for a given resource ID.

        This method creates a list of potential archive file names where the
        requested resource might be located.

        :param resource_id: The ID of the resource to look for.
        :type resource_id: int
        :return: An iterable of possible archive file names.
        :rtype: Iterable[str]
        """
        modulo = f'{resource_id % 1000:03d}'
        return [
            f'original/data-0{modulo}.tar',
            f'recent/data-1{modulo}.tar',
            *[file for file in self._get_update_files() if
              fnmatch.fnmatch(os.path.basename(file), f'*-{resource_id % 10}.tar')]
        ]


class DanbooruDataPool(_BaseDanbooruDataPool):
    """
    Main class for accessing Danbooru image data.

    This class provides access to the Danbooru dataset using default repository IDs.

    :param data_revision: The revision of the data repository to use. Defaults to 'main'.
    :type data_revision: str
    :param idx_revision: The revision of the index repository to use. Defaults to 'main'.
    :type idx_revision: str
    :param hf_token: Optional Hugging Face token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, data_revision: str = 'main', idx_revision: str = 'main', hf_token: Optional[str] = None):
        _BaseDanbooruDataPool.__init__(
            self,
            data_repo_id=_DEFAULT_DATA_REPO_ID,
            data_revision=data_revision,
            idx_repo_id=_DEFAULT_IDX_REPO_ID,
            idx_revision=idx_revision,
            hf_token=hf_token,
        )


class DanbooruStableDataPool(DanbooruDataPool):
    """
    Class for accessing a stable version of Danbooru data.

    This class uses specific revisions of the data and index repositories
    to provide access to a stable version of the Danbooru dataset.

    :param hf_token: Optional Hugging Face token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, hf_token: Optional[str] = None):
        DanbooruDataPool.__init__(
            self,
            data_revision='81652caef9403712b4112a3fcb5d9b4997424ac3',
            idx_revision='20240319',
            hf_token=hf_token,
        )


_N_REPO_ID = 'deepghs/danbooru_newest'


class _DanbooruNewestPartialDataPool(IncrementIDDataPool):
    """
    Class for accessing the newest partial Danbooru data.

    This class provides access to the most recent additions to the Danbooru dataset.

    :param data_revision: The revision of the data repository to use. Defaults to 'main'.
    :type data_revision: str
    :param idx_revision: The revision of the index repository to use. Defaults to 'main'.
    :type idx_revision: str
    :param hf_token: Optional Hugging Face token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, data_revision: str = 'main', idx_revision: str = 'main', hf_token: Optional[str] = None):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_N_REPO_ID,
            data_revision=data_revision,
            idx_repo_id=_N_REPO_ID,
            idx_revision=idx_revision,
            hf_token=hf_token,
        )


class DanbooruNewestDataPool(DataPool):
    """
    Class for accessing both stable and newest Danbooru data.

    This class combines access to both the stable Danbooru dataset and
    the newest additions, providing a comprehensive view of the data.

    :param hf_token: Optional Hugging Face token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, hf_token: Optional[str] = None):
        self._old_pool = DanbooruStableDataPool(hf_token=hf_token)
        self._newest_pool = _DanbooruNewestPartialDataPool(hf_token=hf_token)

    @contextmanager
    def mock_resource(self, resource_id, resource_info, silent: bool = False) -> ContextManager[Tuple[str, Any]]:
        """
        Provide a context manager for accessing a resource.

        This method attempts to retrieve the resource from both the stable and newest
        data pools, returning the first successful match.

        :param resource_id: The ID of the resource to retrieve.
        :type resource_id: Any
        :param resource_info: Additional information about the resource.
        :type resource_info: Any
        :param silent: If True, suppresses progress bar of each standalone files during the mocking process.
        :type silent: bool
        :return: A context manager yielding a tuple of (temporary directory, resource info).
        :rtype: ContextManager[Tuple[str, Any]]
        :raises ResourceNotFoundError: If the resource is not found in either pool.
        """
        pools = [self._old_pool, self._newest_pool]
        found = False
        for pool in pools:
            try:
                with pool.mock_resource(resource_id, resource_info, silent=silent) as (td, info):
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
    """
    Class for accessing WebP versions of Danbooru images.

    This class provides access to WebP-formatted Danbooru images, which are
    typically smaller in file size while maintaining good quality.

    :param data_revision: The revision of the data repository to use. Defaults to 'main'.
    :type data_revision: str
    :param idx_revision: The revision of the index repository to use. Defaults to 'main'.
    :type idx_revision: str
    :param hf_token: Optional Hugging Face token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, data_revision: str = 'main', idx_revision: str = 'main', hf_token: Optional[str] = None):
        _BaseDanbooruDataPool.__init__(
            self,
            data_repo_id=_DEFAULT_WEBP_DATA_REPO_ID,
            data_revision=data_revision,
            idx_repo_id=_DEFAULT_WEBP_IDX_REPO_ID,
            idx_revision=idx_revision,
            hf_token=hf_token,
        )

    def _request_possible_archives(self, resource_id) -> Iterable[str]:
        """
        Generate a list of possible archive names for a given resource ID.

        This method overrides the base class method to provide WebP-specific
        archive file names.

        :param resource_id: The ID of the resource to look for.
        :type resource_id: int
        :return: An iterable of possible archive file names.
        :rtype: Iterable[str]
        """
        modulo = f'{resource_id % 1000:03d}'
        return [
            f'images/data-0{modulo}.tar',
            f'images/data-1{modulo}.tar',
            *[file for file in self._get_update_files() if
              fnmatch.fnmatch(os.path.basename(file), f'*-{resource_id % 10}.tar')]
        ]


_N_WEBP_RPEO_ID = 'deepghs/danbooru_newest-webp-4Mpixel'


class _DanbooruNewestPartialWebpDataPool(IncrementIDDataPool):
    """
    Class for accessing the newest partial WebP Danbooru data.

    This class provides access to the most recent additions to the WebP-formatted
    Danbooru dataset.

    :param data_revision: The revision of the data repository to use. Defaults to 'main'.
    :type data_revision: str
    :param idx_revision: The revision of the index repository to use. Defaults to 'main'.
    :type idx_revision: str
    :param hf_token: Optional Hugging Face token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, data_revision: str = 'main', idx_revision: str = 'main', hf_token: Optional[str] = None):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_N_WEBP_RPEO_ID,
            data_revision=data_revision,
            idx_repo_id=_N_WEBP_RPEO_ID,
            idx_revision=idx_revision,
            hf_token=hf_token,
        )


class DanbooruNewestWebpDataPool(DataPool):
    """
    Class for accessing both stable and newest WebP Danbooru data.

    This class combines access to both the stable WebP-formatted Danbooru dataset
    and the newest WebP additions, providing a comprehensive view of the WebP data.

    :param hf_token: Optional Hugging Face token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, hf_token: Optional[str] = None):
        self._old_pool = DanbooruWebpDataPool(hf_token=hf_token)
        self._newest_pool = _DanbooruNewestPartialWebpDataPool(hf_token=hf_token)

    @contextmanager
    def mock_resource(self, resource_id, resource_info, silent: bool = False) -> ContextManager[Tuple[str, Any]]:
        """
        Provide a context manager for accessing a WebP resource.

        This method attempts to retrieve the WebP resource from both the stable and newest
        WebP data pools, returning the first successful match.

        :param resource_id: The ID of the resource to retrieve.
        :type resource_id: Any
        :param resource_info: Additional information about the resource.
        :type resource_info: Any
        :param silent: If True, suppresses progress bar of each standalone files during the mocking process.
        :type silent: bool
        :return: A context manager yielding a tuple of (temporary directory, resource info).
        :rtype: ContextManager[Tuple[str, Any]]
        :raises ResourceNotFoundError: If the resource is not found in either WebP pool.
        """
        pools = [self._old_pool, self._newest_pool]
        found = False
        for pool in pools:
            try:
                with pool.mock_resource(resource_id, resource_info, silent=silent) as (td, info):
                    yield td, info
            except ResourceNotFoundError:
                pass
            else:
                found = True
                break

        if not found:
            raise ResourceNotFoundError(f'Resource {resource_id!r} not found.')
