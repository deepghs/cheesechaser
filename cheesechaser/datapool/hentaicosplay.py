"""
This module provides functionality for managing and accessing a Hentai Cosplay data pool.

The module includes a class `HentaiCosplayDataPool` which extends `IncrementIDDataPool`.
It's designed to handle data stored in a specific structure within a Hugging Face repository.
The data is organized in archive files (.tar) with a hierarchical directory structure based on resource IDs.

Key features:

- Manages access to data stored in Hugging Face repositories
- Implements a natural sorting algorithm for archive directories
- Provides methods to locate and retrieve specific resources based on their IDs

.. note::
    The dataset `deepghs/hentai_cosplay_trans <https://huggingface.co/datasets/deepghs/hentai_cosplay_trans>`_
    is private, you have to get the access of it before using this module.
"""

import os
from typing import Iterable, Optional

from hfutils.operate import get_hf_fs
from natsort import natsorted

from .base import IncrementIDDataPool, id_modulo_cut

_HC_REPO = 'deepghs/hentai_cosplay_trans'


class HentaiCosplayDataPool(IncrementIDDataPool):
    """
    A class representing a data pool for Hentai Cosplay resources.

    This class extends IncrementIDDataPool and provides specific functionality
    for managing and accessing Hentai Cosplay data stored in Hugging Face repositories.

    :param repo_id: The ID of the Hugging Face repository containing the data.
    :type repo_id: str
    :param revision: The revision of the repository to use.
    :type revision: str
    :param base_level: The base level for ID modulo operations.
    :type base_level: int
    :param hf_token: Optional Hugging Face authentication token.
    :type hf_token: Optional[str]
    """

    def __init__(self, repo_id: str = _HC_REPO, revision: str = 'main', base_level: int = 3,
                 hf_token: Optional[str] = None):
        """
        Initialize the HentaiCosplayDataPool.

        :param repo_id: The ID of the Hugging Face repository containing the data.
        :type repo_id: str
        :param revision: The revision of the repository to use.
        :type revision: str
        :param base_level: The base level for ID modulo operations.
        :type base_level: int
        :param hf_token: Optional Hugging Face authentication token.
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=repo_id, data_revision=revision,
            idx_repo_id=repo_id, idx_revision=revision,
            base_level=base_level,
            hf_token=hf_token,
        )
        self._archive_dirs = None

    def _get_archive_dirs(self):
        """
        Retrieve and cache the list of archive directories.

        This method uses the Hugging Face filesystem to glob all .tar files
        in the dataset repository and extracts their directory paths.
        The paths are then natural-sorted for consistent ordering.

        :return: A list of archive directory paths.
        :rtype: list

        Note:
            This method caches the result for subsequent calls.
        """
        if self._archive_dirs is None:
            hf_fs = get_hf_fs(hf_token=self._hf_token)
            self._archive_dirs = natsorted({
                os.path.dirname(os.path.relpath(file, f'datasets/{self.data_repo_id}'))
                for file in hf_fs.glob(f'datasets/{self.data_repo_id}/**/*.tar')
            })

        return self._archive_dirs

    def _request_possible_archives(self, resource_id) -> Iterable[str]:
        """
        Generate possible archive paths for a given resource ID.

        This method calculates the modulo of the resource ID and uses it to construct
        possible archive paths where the resource might be located.

        :param resource_id: The ID of the resource to locate.
        :type resource_id: int
        :return: An iterable of possible archive paths.
        :rtype: Iterable[str]

        Note:
            The method uses the base_level attribute to determine the modulo calculation
            and directory structure.
        """
        modulo = resource_id % (10 ** self.base_level)
        modulo_str = str(modulo)
        if len(modulo_str) < self.base_level:
            modulo_str = '0' * (self.base_level - len(modulo_str)) + modulo_str

        modulo_segments = id_modulo_cut(modulo_str)
        modulo_segments[-1] = f'{modulo_segments[-1]}'
        return [
            f'{base_dir}/{"/".join(modulo_segments)}.tar'
            for base_dir in self._get_archive_dirs()
        ]
