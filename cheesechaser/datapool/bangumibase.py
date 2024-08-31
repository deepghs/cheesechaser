"""
This module provides a data pool implementation for the BangumiBase dataset.

The BangumiBase dataset is a comprehensive collection of anime and manga information.
This module extends the IncrementIDDataPool to specifically handle the BangumiBase dataset,
providing an easy-to-use interface for accessing and managing this data.

.. note::
    The dataset `deepghs/bangumibase_full <https://huggingface.co/datasets/deepghs/bangumibase_full>`_
    is gated, you have to get the access of it before using this module.
"""

from typing import Optional

from .base import IncrementIDDataPool

_BANGUMIBASE_REPO = 'deepghs/bangumibase_full'


class BangumiBaseDataPool(IncrementIDDataPool):
    """
    A data pool class for managing and accessing the BangumiBase dataset.

    This class extends the IncrementIDDataPool to provide specific functionality
    for the BangumiBase dataset. It simplifies the process of initializing the
    data pool with the correct repository and revision information.

    The BangumiBaseDataPool allows users to easily interact with the BangumiBase
    dataset, providing methods for retrieving, updating, and managing anime and
    manga information.

    :param revision: The specific revision of the BangumiBase dataset to use.
                     Defaults to 'main'.
    :type revision: str
    :param hf_token: An optional Hugging Face token for accessing private repositories.
                     Defaults to None.
    :type hf_token: Optional[str]

    :example:

    To create a BangumiBaseDataPool instance:

    >>> pool = BangumiBaseDataPool()
    >>> # Or with a specific revision
    >>> pool = BangumiBaseDataPool(revision='v1.2.3')
    >>> # Or with a Hugging Face token
    >>> pool = BangumiBaseDataPool(hf_token='your_hf_token_here')
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the BangumiBaseDataPool.

        This constructor sets up the data pool with the BangumiBase dataset repository
        and the specified revision. It uses the same repository for both data and index.

        :param revision: The specific revision of the BangumiBase dataset to use.
                         Defaults to 'main'.
        :type revision: str
        :param hf_token: An optional Hugging Face token for accessing private repositories.
                         Defaults to None.
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_BANGUMIBASE_REPO,
            data_revision=revision,
            idx_repo_id=_BANGUMIBASE_REPO,
            idx_revision=revision,
            hf_token=hf_token,
        )
