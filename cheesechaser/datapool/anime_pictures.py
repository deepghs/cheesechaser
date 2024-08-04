"""
This module provides a data pool implementation for anime pictures.

It defines a class `AnimePicturesDataPool` which inherits from `IncrementIDDataPool`.
This class is designed to manage and access a repository of anime pictures,
utilizing an incremental ID system for efficient data retrieval.
"""

from .base import IncrementIDDataPool

_ANIME_PICTURES_REPO = 'deepghs/anime_pictures_full'


class AnimePicturesDataPool(IncrementIDDataPool):
    """
    A data pool class for managing and accessing anime pictures.

    This class extends the IncrementIDDataPool to provide specific functionality
    for handling anime picture data. It uses a predefined repository for storing
    and retrieving anime pictures.

    :param revision: The revision of the data to use, defaults to 'main'.
    :type revision: str

    Usage:
        >>> pool = AnimePicturesDataPool()
        >>> pool = AnimePicturesDataPool(revision='v1.0')

    .. note::
        The class uses the same repository for both data and index storage.
    """

    def __init__(self, revision: str = 'main'):
        """
        Initialize the AnimePicturesDataPool.

        :param revision: The revision of the data to use, defaults to 'main'.
        :type revision: str
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_ANIME_PICTURES_REPO,
            data_revision=revision,
            idx_repo_id=_ANIME_PICTURES_REPO,
            idx_revision=revision,
        )
