"""
This module provides data pool classes for accessing Sankaku image data.

It contains two classes:

1. SankakuDataPool: For accessing the full Sankaku dataset.
2. SankakuWebpDataPool: For accessing the WebP-formatted Sankaku dataset with 4M pixel images.

Both classes inherit from IncrementIDDataPool and provide easy access to the respective datasets
stored in Hugging Face repositories. These classes simplify the process of retrieving and working
with Sankaku image data, allowing users to easily integrate this data into their projects or
research.

.. note::
    The datasets `deepghs/sankaku_full <https://huggingface.co/datasets/deepghs/sankaku_full>`_ and
    `deepghs/sankaku-webp-4Mpixel <https://huggingface.co/datasets/deepghs/sankaku-webp-4Mpixel>`_
    is gated, you have to get the access of it before using this module.
"""

import os.path
from collections import defaultdict
from threading import Lock
from typing import Dict, List, Iterable, Optional

from hfutils.operate import get_hf_fs
from hfutils.utils import parse_hf_fs_path, hf_fs_path
from natsort import natsorted

from .base import IncrementIDDataPool

_SANKAKU_REPO = 'deepghs/sankaku_full'


class SankakuDataPool(IncrementIDDataPool):
    """
    A data pool class for accessing the full Sankaku dataset.

    This class inherits from IncrementIDDataPool and is configured to access
    the full Sankaku dataset stored in the 'deepghs/sankaku_full' repository.
    It provides methods to retrieve image data based on image IDs.

    :param revision: The revision of the dataset to use, defaults to 'main'.
    :type revision: str

    Note:
        This class uses a base level of 4 for file organization, which means
        the images are stored in a directory structure with 4 levels of subdirectories.
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the SankakuDataPool.

        :param revision: The revision of the dataset to use, defaults to 'main'.
        :type revision: str
        :param hf_token: Hugging Face authentication token, defaults to None.
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_SANKAKU_REPO,
            data_revision=revision,
            idx_repo_id=_SANKAKU_REPO,
            idx_revision=revision,
            base_level=[3, 4],
            hf_token=hf_token,
        )
        self._tar_files = None
        self._lock = Lock()

    def _get_tar_files(self) -> Dict[str, List[str]]:
        with self._lock:
            if self._tar_files is None:
                hf_fs = get_hf_fs(hf_token=self._hf_token)
                _tar_files = natsorted([
                    parse_hf_fs_path(file).filename
                    for file in hf_fs.glob(hf_fs_path(
                        repo_id=self.data_repo_id,
                        repo_type='dataset',
                        filename='images/**/*.tar',
                        revision=self.data_revision,
                    ))
                ])

                self._tar_files = defaultdict(list)
                for tar_file in _tar_files:
                    self._tar_files[os.path.basename(tar_file)].append(tar_file)

        return self._tar_files

    def _request_possible_archives(self, resource_id) -> Iterable[str]:
        modulo = f'{resource_id % 1000:03d}'
        return self._get_tar_files()[f'0{modulo}.tar']


_SANKAKU_WEBP_REPO = 'deepghs/sankaku-webp-4Mpixel'


class SankakuWebpDataPool(IncrementIDDataPool):
    """
    A data pool class for accessing the WebP-formatted Sankaku dataset with 4M pixel images.

    This class inherits from IncrementIDDataPool and is configured to access
    the WebP-formatted Sankaku dataset stored in the 'deepghs/sankaku-webp-4Mpixel' repository.
    It provides methods to retrieve WebP-formatted image data based on image IDs.

    :param revision: The revision of the dataset to use, defaults to 'main'.
    :type revision: str
    :param hf_token: Hugging Face authentication token, defaults to None.
    :type hf_token: Optional[str]

    Note:
        This class uses a base level of 3 for file organization, which means
        the images are stored in a directory structure with 3 levels of subdirectories.
        Authentication may be required to access this dataset.
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the SankakuWebpDataPool.

        :param revision: The revision of the dataset to use, defaults to 'main'.
        :type revision: str
        :param hf_token: Hugging Face authentication token, defaults to None.
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_SANKAKU_WEBP_REPO,
            data_revision=revision,
            idx_repo_id=_SANKAKU_WEBP_REPO,
            idx_revision=revision,
            base_level=3,
            hf_token=hf_token,
        )
