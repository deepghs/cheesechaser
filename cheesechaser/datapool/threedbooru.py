"""
This module provides a data pool implementation for the 3D Booru dataset.

It includes a class `ThreedbooruDataPool` which extends the `IncrementIDDataPool`
to specifically handle the 3D Booru dataset. This module allows users to easily
access and manage data from the 3D Booru repository hosted on Hugging Face.

.. note::
    The dataset `deepghs/3dbooru_full <https://huggingface.co/datasets/deepghs/3dbooru_full>`_
    is gated, you have to get the access of it before using this module.
"""

from typing import Optional

from .base import IncrementIDDataPool

_3DBOORU_REPO = 'deepghs/3dbooru_full'


class ThreedbooruDataPool(IncrementIDDataPool):
    """
    A data pool class for managing and accessing the 3D Booru dataset.

    This class extends the `IncrementIDDataPool` to provide specific functionality
    for the 3D Booru dataset. It uses a predefined repository ID for both data
    and index information.

    :param revision: The specific revision of the 3D Booru dataset to use, defaults to 'main'.
    :type revision: str
    :param hf_token: Optional Hugging Face authentication token for accessing private repositories.
    :type hf_token: Optional[str]

    Usage:
        >>> pool = ThreedbooruDataPool()
        >>> pool_with_token = ThreedbooruDataPool(hf_token='your_token_here')

    .. note::
        The 3D Booru dataset is hosted in the 'deepghs/3dbooru_full' repository on Hugging Face.
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the ThreedbooruDataPool.

        This constructor sets up the data pool with the specified revision of the 3D Booru dataset.
        It uses the same repository for both data and index information.

        :param revision: The specific revision of the 3D Booru dataset to use, defaults to 'main'.
        :type revision: str
        :param hf_token: Optional Hugging Face authentication token for accessing private repositories.
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_3DBOORU_REPO,
            data_revision=revision,
            idx_repo_id=_3DBOORU_REPO,
            idx_revision=revision,
            hf_token=hf_token,
        )
