"""
This module provides a data pool implementation for anime pictures.

It defines a class `AnimePicturesDataPool` which inherits from `IncrementIDDataPool`.
This class is designed to manage and access a repository of anime pictures,
utilizing an incremental ID system for efficient data retrieval.

.. note::
    The dataset `deepghs/anime_pictures_full <https://huggingface.co/datasets/deepghs/anime_pictures_full>`_
    is gated, you have to get the access of it before using this module.
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
        >>> pool = AnimePicturesDataPool(revision='v1.0')
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
