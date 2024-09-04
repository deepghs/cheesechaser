"""
This module provides data pool implementations for Konachan images.

It defines two classes:

1. KonachanDataPool: A specialized version of IncrementIDDataPool for handling Konachan image data.
2. KonachanWebpDataPool: A specialized version of IncrementIDDataPool for handling Konachan WebP image data.

The module uses predefined repositories for storing and accessing Konachan image data.

.. note::
    The datasets `deepghs/konachan_full <https://huggingface.co/datasets/deepghs/konachan_full>`_ and
    `deepghs/konachan-webp-4Mpixel <https://huggingface.co/datasets/deepghs/konachan-webp-4Mpixel>`_ are gated.
    You have to get access to them before using this module.
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
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_KONACHAN_REPO,
            data_revision=revision,
            idx_repo_id=_KONACHAN_REPO,
            idx_revision=revision,
            hf_token=hf_token,
        )


_KONACHAN_WEBP_REPO = 'deepghs/konachan-webp-4Mpixel'


class KonachanWebpDataPool(IncrementIDDataPool):
    """
    A data pool class for managing Konachan WebP image data.

    This class extends the IncrementIDDataPool to provide specific functionality
    for handling Konachan WebP image data. It uses a predefined repository to store
    and access the WebP image data and indices.

    :param revision: The revision of the data to use, defaults to 'main'.
    :type revision: str
    :param hf_token: Optional Hugging Face token for authentication, defaults to None.
    :type hf_token: Optional[str]

    Usage:
        >>> konachan_webp_pool = KonachanWebpDataPool()
        >>> konachan_webp_pool_with_token = KonachanWebpDataPool(hf_token='your_token_here')
        >>> konachan_webp_pool_specific_revision = KonachanWebpDataPool(revision='v1.0')

    Note:
        The KonachanWebpDataPool uses a predefined repository (_KONACHAN_WEBP_REPO) for both
        data and index storage. This ensures consistency in data access and management
        for Konachan WebP images.
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the KonachanWebpDataPool.

        This constructor sets up the KonachanWebpDataPool with the specified revision
        and optional Hugging Face token. It initializes the underlying IncrementIDDataPool
        with the predefined Konachan WebP repository for both data and index management.

        :param revision: The revision of the data to use, defaults to 'main'.
        :type revision: str
        :param hf_token: Optional Hugging Face token for authentication, defaults to None.
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_KONACHAN_WEBP_REPO,
            data_revision=revision,
            idx_repo_id=_KONACHAN_WEBP_REPO,
            idx_revision=revision,
            hf_token=hf_token,
        )
