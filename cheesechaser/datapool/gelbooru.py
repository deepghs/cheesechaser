"""
This module provides data pool classes for accessing Gelbooru image data.

It contains two classes:

1. GelbooruDataPool: For accessing the full Gelbooru dataset.
2. GelbooruWebpDataPool: For accessing the WebP-formatted Gelbooru dataset with 4M pixel images.

Both classes inherit from IncrementIDDataPool and provide easy access to the respective datasets
stored in Hugging Face repositories.
"""

from .base import IncrementIDDataPool

_GELBOORU_REPO = 'deepghs/gelbooru_full'


class GelbooruDataPool(IncrementIDDataPool):
    """
    A data pool class for accessing the full Gelbooru dataset.

    This class inherits from IncrementIDDataPool and is configured to access
    the full Gelbooru dataset stored in the 'deepghs/gelbooru_full' repository.

    :param revision: The revision of the dataset to use, defaults to 'main'.
    :type revision: str
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
            base_level=4,
        )


_GELBOORU_WEBP_REPO = 'deepghs/gelbooru-webp-4Mpixel'


class GelbooruWebpDataPool(IncrementIDDataPool):
    """
    A data pool class for accessing the WebP-formatted Gelbooru dataset with 4M pixel images.

    This class inherits from IncrementIDDataPool and is configured to access
    the WebP-formatted Gelbooru dataset stored in the 'deepghs/gelbooru-webp-4Mpixel' repository.

    :param revision: The revision of the dataset to use, defaults to 'main'.
    :type revision: str
    """

    def __init__(self, revision: str = 'main'):
        """
        Initialize the GelbooruWebpDataPool.

        :param revision: The revision of the dataset to use, defaults to 'main'.
        :type revision: str
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_GELBOORU_WEBP_REPO,
            data_revision=revision,
            idx_repo_id=_GELBOORU_WEBP_REPO,
            idx_revision=revision,
            base_level=3,
        )
