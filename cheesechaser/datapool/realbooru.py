"""
This module provides a data pool implementation for Realbooru dataset.

The RealbooruDataPool class extends the IncrementIDDataPool to specifically handle
the Realbooru dataset, which is stored in a Hugging Face repository.

.. note::
    The dataset `deepghs/realbooru_full <https://huggingface.co/datasets/deepghs/realbooru_full>`_
    is gated, you have to get the access of it before using this module.
"""

from typing import Optional

from .base import IncrementIDDataPool

_REALBOORU_REPO = 'deepghs/realbooru_full'


class RealbooruDataPool(IncrementIDDataPool):
    """
    A data pool class for accessing and managing Realbooru dataset.

    This class inherits from IncrementIDDataPool and is specifically designed to work
    with the Realbooru dataset stored in a Hugging Face repository. It provides an
    interface to access and manage the data using incremental IDs.

    The Realbooru dataset is a large collection of images and associated metadata,
    commonly used for machine learning tasks in computer vision and image processing.

    :param revision: The specific revision of the Realbooru dataset to use, defaults to 'main'.
    :type revision: str
    :param hf_token: Optional Hugging Face authentication token for accessing private repositories.
    :type hf_token: Optional[str]
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the RealbooruDataPool.

        This constructor sets up the data pool by specifying the Hugging Face repository
        and revision for both the data and index. It uses the _REALBOORU_REPO constant
        as the repository ID for both data and index.

        :param revision: The specific revision of the Realbooru dataset to use, defaults to 'main'.
                         This allows for version control of the dataset.
        :type revision: str
        :param hf_token: Optional Hugging Face authentication token for accessing private repositories.
                         If provided, it enables access to private datasets.
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_REALBOORU_REPO,
            data_revision=revision,
            idx_repo_id=_REALBOORU_REPO,
            idx_revision=revision,
            hf_token=hf_token,
        )
