"""
This module provides data pool classes for accessing Gelbooru image data.

It contains two classes:

1. GelbooruDataPool: For accessing the full Gelbooru dataset.
2. GelbooruWebpDataPool: For accessing the WebP-formatted Gelbooru dataset with 4M pixel images.

Both classes inherit from IncrementIDDataPool and provide easy access to the respective datasets
stored in Hugging Face repositories. These classes simplify the process of retrieving and working
with Gelbooru image data, allowing users to easily integrate this data into their projects or
research.

.. note::
    The datasets `deepghs/gelbooru_full <https://huggingface.co/datasets/deepghs/gelbooru_full>`_ and
    `deepghs/gelbooru-webp-4Mpixel <https://huggingface.co/datasets/deepghs/gelbooru-webp-4Mpixel>`_
    is gated, you have to get the access of it before using this module.
"""

from typing import Optional

from .base import IncrementIDDataPool

_GELBOORU_REPO = 'deepghs/gelbooru_full'


class GelbooruDataPool(IncrementIDDataPool):
    """
    A data pool class for accessing the full Gelbooru dataset.

    This class inherits from IncrementIDDataPool and is configured to access
    the full Gelbooru dataset stored in the 'deepghs/gelbooru_full' repository.
    It provides methods to retrieve image data based on image IDs.

    :param revision: The revision of the dataset to use, defaults to 'main'.
    :type revision: str

    Note:
        This class uses a base level of 4 for file organization, which means
        the images are stored in a directory structure with 4 levels of subdirectories.
    """

    def __init__(self, revision: str = 'main'):
        """
        Initialize the GelbooruDataPool.

        :param revision: The revision of the dataset to use, defaults to 'main'.
        :type revision: str
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_GELBOORU_REPO,
            data_revision=revision,
            idx_repo_id=_GELBOORU_REPO,
            idx_revision=revision,
            base_level=[3, 4],
        )


_GELBOORU_WEBP_REPO = 'deepghs/gelbooru-webp-4Mpixel'


class GelbooruWebpDataPool(IncrementIDDataPool):
    """
    A data pool class for accessing the WebP-formatted Gelbooru dataset with 4M pixel images.

    This class inherits from IncrementIDDataPool and is configured to access
    the WebP-formatted Gelbooru dataset stored in the 'deepghs/gelbooru-webp-4Mpixel' repository.
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
        Initialize the GelbooruWebpDataPool.

        :param revision: The revision of the dataset to use, defaults to 'main'.
        :type revision: str
        :param hf_token: Hugging Face authentication token, defaults to None.
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_GELBOORU_WEBP_REPO,
            data_revision=revision,
            idx_repo_id=_GELBOORU_WEBP_REPO,
            idx_revision=revision,
            base_level=3,
            hf_token=hf_token,
        )
