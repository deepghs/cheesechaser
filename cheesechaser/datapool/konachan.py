"""
This module provides a data pool implementation for Konachan images.

It defines the KonachanDataPool class, which is a specialized version of the
IncrementIDDataPool for handling Konachan image data. The module uses a
predefined repository for storing and accessing Konachan image data.

.. note::
    The dataset `deepghs/konachan_full <https://huggingface.co/datasets/deepghs/konachan_full>`_
    is gated, you have to get the access of it before using this module.
"""

from typing import Optional

from .base import IncrementIDDataPool

_KONACHAN_REPO = 'deepghs/konachan_full'


class KonachanDataPool(IncrementIDDataPool):
    """
    A data pool class for managing Konachan image data.

    This class extends the IncrementIDDataPool to provide specific functionality
    for handling Konachan image data. It uses a predefined repository to store
    and access the image data and indices.

    :param revision: The revision of the data to use, defaults to 'main'.
    :type revision: str
    :param hf_token: Optional Hugging Face token for authentication, defaults to None.
    :type hf_token: Optional[str]

    Usage:
        >>> konachan_pool = KonachanDataPool()
        >>> konachan_pool_with_token = KonachanDataPool(hf_token='your_token_here')
        >>> konachan_pool_specific_revision = KonachanDataPool(revision='v1.0')

    Note:
        The KonachanDataPool uses a predefined repository (_KONACHAN_REPO) for both
        data and index storage. This ensures consistency in data access and management
        for Konachan images.
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the KonachanDataPool.

        This constructor sets up the KonachanDataPool with the specified revision
        and optional Hugging Face token. It initializes the underlying IncrementIDDataPool
        with the predefined Konachan repository for both data and index management.

        :param revision: The revision of the data to use, defaults to 'main'.
        :type revision: str
        :param hf_token: Optional Hugging Face token for authentication, defaults to None.
        :type hf_token: Optional[str]

        Example:
            >>> pool = KonachanDataPool(revision='v2.0', hf_token='my_secret_token')
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_KONACHAN_REPO,
            data_revision=revision,
            idx_repo_id=_KONACHAN_REPO,
            idx_revision=revision,
            hf_token=hf_token,
        )
