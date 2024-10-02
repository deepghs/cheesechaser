"""
This module provides classes for accessing and managing E621 image data.

It includes classes for handling both original and WebP-formatted images,
as well as stable and newest datasets. The module uses Hugging Face's
file system for data storage and retrieval.

Classes:

- BaseE621DataPool: Base class for E621 data pools.
- E621DataPool: Main class for accessing E621 image data.
- E621StableDataPool: Class for accessing a stable version of E621 data.
- E621NewestPartialDataPool: Class for accessing the newest partial E621 data.
- E621NewestDataPool: Class for accessing both stable and newest E621 data.
- E621WebpDataPool: Class for accessing WebP versions of E621 images.
- E621NewestPartialWebpDataPool: Class for accessing the newest partial WebP E621 data.
- E621NewestWebpDataPool: Class for accessing both stable and newest WebP E621 data.

The module uses several constants to define default repository IDs and revisions.
"""

from contextlib import contextmanager
from typing import Iterable, ContextManager, Tuple, Any, Optional

from .base import DataPool, ResourceNotFoundError, IncrementIDDataPool

_DEFAULT_DATA_REPO_ID = 'boxingscorpionbagel/e621-2024'
_DEFAULT_IDX_REPO_ID = 'deepghs/e621-2024_index'


class _BaseE621DataPool(IncrementIDDataPool):
    """
    Base class for E621 data pools.

    This class extends IncrementIDDataPool and provides common functionality
    for E621 data access.

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
        return [f'original/data-0{modulo}.tar']


class E621DataPool(_BaseE621DataPool):
    """
    Main class for accessing E621 image data.

    This class provides access to the E621 dataset using default repository IDs.

    :param data_revision: The revision of the data repository to use. Defaults to 'main'.
    :type data_revision: str
    :param idx_revision: The revision of the index repository to use. Defaults to 'main'.
    :type idx_revision: str
    :param hf_token: Optional Hugging Face token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, data_revision: str = 'main', idx_revision: str = 'main', hf_token: Optional[str] = None):
        _BaseE621DataPool.__init__(
            self,
            data_repo_id=_DEFAULT_DATA_REPO_ID,
            data_revision=data_revision,
            idx_repo_id=_DEFAULT_IDX_REPO_ID,
            idx_revision=idx_revision,
            hf_token=hf_token,
        )


class E621StableDataPool(E621DataPool):
    """
    Class for accessing a stable version of E621 data.

    This class uses specific revisions of the data and index repositories
    to provide access to a stable version of the E621 dataset.

    :param hf_token: Optional Hugging Face token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, hf_token: Optional[str] = None):
        E621DataPool.__init__(
            self,
            data_revision='9ee7b0ace34c8e06491e77bde071cb82f17a6886',
            idx_revision='main',
            hf_token=hf_token,
        )


_N_REPO_ID = 'deepghs/e621_newest'


class _E621NewestPartialDataPool(IncrementIDDataPool):
    """
    Class for accessing the newest partial E621 data.

    This class provides access to the most recent additions to the E621 dataset.

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


class E621NewestDataPool(DataPool):
    """
    Class for accessing both stable and newest E621 data.

    This class combines access to both the stable E621 dataset and
    the newest additions, providing a comprehensive view of the data.

    :param hf_token: Optional Hugging Face token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, hf_token: Optional[str] = None):
        self._old_pool = E621StableDataPool(hf_token=hf_token)
        self._newest_pool = _E621NewestPartialDataPool(hf_token=hf_token)

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


_DEFAULT_WEBP_DATA_REPO_ID = 'NebulaeWis/e621-2024-webp-4Mpixel'
_DEFAULT_WEBP_IDX_REPO_ID = 'deepghs/e621-2024-webp-4Mpixel_index'


class E621WebpDataPool(_BaseE621DataPool):
    """
    Class for accessing WebP versions of E621 images.

    This class provides access to WebP-formatted E621 images, which are
    typically smaller in file size while maintaining good quality.

    :param data_revision: The revision of the data repository to use. Defaults to 'main'.
    :type data_revision: str
    :param idx_revision: The revision of the index repository to use. Defaults to 'main'.
    :type idx_revision: str
    :param hf_token: Optional Hugging Face token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, data_revision: str = 'main', idx_revision: str = 'main', hf_token: Optional[str] = None):
        _BaseE621DataPool.__init__(
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
        return [f'original/data-0{modulo}.tar']


_N_WEBP_RPEO_ID = 'deepghs/e621_newest-webp-4Mpixel'


class _E621NewestPartialWebpDataPool(IncrementIDDataPool):
    """
    Class for accessing the newest partial WebP E621 data.

    This class provides access to the most recent additions to the WebP-formatted
    E621 dataset.

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


class E621NewestWebpDataPool(DataPool):
    """
    Class for accessing both stable and newest WebP E621 data.

    This class combines access to both the stable WebP-formatted E621 dataset
    and the newest WebP additions, providing a comprehensive view of the WebP data.

    :param hf_token: Optional Hugging Face token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, hf_token: Optional[str] = None):
        self._old_pool = E621WebpDataPool(hf_token=hf_token)
        self._newest_pool = _E621NewestPartialWebpDataPool(hf_token=hf_token)

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
