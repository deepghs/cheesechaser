"""
This module provides data pool classes for accessing Zerochan image datasets.

It includes two main classes:

1. ZerochanDataPool: For accessing the full Zerochan dataset.
2. ZerochanWebpDataPool: For accessing the WebP-formatted Zerochan dataset.

These classes inherit from IncrementIDDataPool and provide easy access to
Zerochan images stored in Hugging Face repositories.

.. note::
    The datasets `deepghs/zerochan_full <https://huggingface.co/datasets/deepghs/zerochan_full>`_
    and `deepghs/zerochan-webp-4Mpixel <https://huggingface.co/datasets/deepghs/zerochan-webp-4Mpixel>`_
    is gated, you have to get the access of it before using this module.
"""

from typing import Optional

from .base import IncrementIDDataPool

_ZEROCHAN_REPO = 'deepghs/zerochan_full'


class ZerochanDataPool(IncrementIDDataPool):
    """
    A data pool for accessing the full Zerochan image dataset.

    This class provides access to Zerochan images stored in the 'deepghs/zerochan_full'
    Hugging Face repository. It uses an incremental ID system for efficient data retrieval.

    :param revision: The revision of the Hugging Face repository to use, defaults to 'main'.
    :type revision: str
    :param hf_token: Optional Hugging Face authentication token for accessing private repositories.
    :type hf_token: Optional[str]
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the ZerochanDataPool.

        :param revision: The revision of the Hugging Face repository to use, defaults to 'main'.
        :type revision: str
        :param hf_token: Optional Hugging Face authentication token for accessing private repositories.
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_ZEROCHAN_REPO,
            data_revision=revision,
            idx_repo_id=_ZEROCHAN_REPO,
            idx_revision=revision,
            base_level=3,
            hf_token=hf_token,
        )


_ZEROCHAN_WEBP_REPO = 'deepghs/zerochan-webp-4Mpixel'


class ZerochanWebpDataPool(IncrementIDDataPool):
    """
    A data pool for accessing the WebP-formatted Zerochan image dataset.

    This class provides access to WebP-formatted Zerochan images stored in the
    'deepghs/zerochan-webp-4Mpixel' Hugging Face repository. It uses an incremental
    ID system for efficient data retrieval.

    :param revision: The revision of the Hugging Face repository to use, defaults to 'main'.
    :type revision: str
    :param hf_token: Optional Hugging Face authentication token for accessing private repositories.
    :type hf_token: Optional[str]
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the ZerochanWebpDataPool.

        :param revision: The revision of the Hugging Face repository to use, defaults to 'main'.
        :type revision: str
        :param hf_token: Optional Hugging Face authentication token for accessing private repositories.
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_ZEROCHAN_WEBP_REPO,
            data_revision=revision,
            idx_repo_id=_ZEROCHAN_WEBP_REPO,
            idx_revision=revision,
            base_level=3,
            hf_token=hf_token,
        )
