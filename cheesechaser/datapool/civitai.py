"""
This module provides a data pool implementation for Civitai, a platform for AI-generated art and models.

The CivitaiDataPool class extends the IncrementIDDataPool to specifically handle data from Civitai.
It uses a predefined repository to store and retrieve data.

Classes:
    CivitaiDataPool: A data pool class for managing Civitai data.

.. note::
    The dataset `deepghs/civitai_full <https://huggingface.co/datasets/deepghs/civitai_full>`_
    is gated, you have to get the access of it before using this module.
"""

from typing import Optional

from .base import IncrementIDDataPool

_CIVITAI_REPO = 'deepghs/civitai_full'


class CivitaiDataPool(IncrementIDDataPool):
    """
    A data pool class specifically designed for handling Civitai data.

    This class extends the IncrementIDDataPool to provide a convenient way to access and manage
    data from the Civitai platform. It uses a predefined repository to store both the data and
    the index information.

    :param revision: The specific revision of the data to use, defaults to 'main'.
    :type revision: str
    :param hf_token: An optional Hugging Face token for authentication, defaults to None.
    :type hf_token: Optional[str]

    Usage:
        >>> civitai_pool = CivitaiDataPool()
        >>> civitai_pool_with_token = CivitaiDataPool(hf_token='your_token_here')
        >>> specific_revision_pool = CivitaiDataPool(revision='v1.0')

    Note:
        The CivitaiDataPool uses a predefined repository (_CIVITAI_REPO) for both data and index storage.
        This ensures consistency and ease of use when working with Civitai data.
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the CivitaiDataPool with the specified revision and optional Hugging Face token.

        :param revision: The specific revision of the data to use, defaults to 'main'.
        :type revision: str
        :param hf_token: An optional Hugging Face token for authentication, defaults to None.
        :type hf_token: Optional[str]

        This method sets up the CivitaiDataPool by initializing the parent IncrementIDDataPool
        with specific parameters tailored for Civitai data. It uses the same repository for
        both data and index storage, ensuring data consistency.
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_CIVITAI_REPO,
            data_revision=revision,
            idx_repo_id=_CIVITAI_REPO,
            idx_revision=revision,
            base_level=4,
            hf_token=hf_token,
        )
