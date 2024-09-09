"""
This module provides data pool implementations for anime pictures.

It defines two classes:

1. `AnimePicturesDataPool`: Manages and accesses a repository of anime pictures.
2. `AnimePicturesWebpDataPool`: Manages and accesses a repository of WebP-formatted anime pictures.

Both classes inherit from `IncrementIDDataPool` and utilize an incremental ID system
for efficient data retrieval.

.. note::
    The datasets used by these classes are gated. You must have access to the
    respective Hugging Face repositories before using this module:

    - `deepghs/anime_pictures_full <https://huggingface.co/datasets/deepghs/anime_pictures_full>`_
    - `deepghs/anime_pictures-webp-4Mpixel <https://huggingface.co/datasets/deepghs/anime_pictures-webp-4Mpixel>`_
"""

from typing import Optional

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
    :param hf_token: Optional Hugging Face token for authentication, defaults to None.
    :type hf_token: Optional[str]

    Usage:
        >>> pool = AnimePicturesDataPool()
        >>> pool = AnimePicturesDataPool(revision='main')
        >>> pool = AnimePicturesDataPool(revision='main', hf_token='your_hf_token')

    .. note::
        The class uses the same repository for both data and index storage.
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the AnimePicturesDataPool.

        This method sets up the data pool by calling the parent class constructor
        with specific parameters for the anime pictures repository.

        :param revision: The revision of the data to use, defaults to 'main'.
        :type revision: str
        :param hf_token: Optional Hugging Face token for authentication, defaults to None.
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_ANIME_PICTURES_REPO,
            data_revision=revision,
            idx_repo_id=_ANIME_PICTURES_REPO,
            idx_revision=revision,
            hf_token=hf_token,
        )


_ANIME_PICTURES_WEBP_REPO = 'deepghs/anime_pictures-webp-4Mpixel'


class AnimePicturesWebpDataPool(IncrementIDDataPool):
    """
    A data pool class for managing and accessing WebP-formatted anime pictures.

    This class extends the IncrementIDDataPool to provide specific functionality
    for handling WebP-formatted anime picture data. It uses a predefined repository
    for storing and retrieving these pictures.

    :param revision: The revision of the data to use, defaults to 'main'.
    :type revision: str
    :param hf_token: Optional Hugging Face token for authentication, defaults to None.
    :type hf_token: Optional[str]

    Usage:
        >>> pool = AnimePicturesWebpDataPool()
        >>> pool = AnimePicturesWebpDataPool(revision='main')
        >>> pool = AnimePicturesWebpDataPool(revision='main', hf_token='your_hf_token')

    .. note::
        The class uses the same repository for both data and index storage.
        The pictures in this pool are in WebP format and have a maximum size of 4 megapixels.
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the AnimePicturesWebpDataPool.

        This method sets up the data pool by calling the parent class constructor
        with specific parameters for the WebP-formatted anime pictures repository.

        :param revision: The revision of the data to use, defaults to 'main'.
        :type revision: str
        :param hf_token: Optional Hugging Face token for authentication, defaults to None.
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_ANIME_PICTURES_WEBP_REPO,
            data_revision=revision,
            idx_repo_id=_ANIME_PICTURES_WEBP_REPO,
            idx_revision=revision,
            hf_token=hf_token,
        )
