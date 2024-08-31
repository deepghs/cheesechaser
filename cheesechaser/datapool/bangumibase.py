"""
This module provides a data pool implementation for the BangumiBase dataset.

The BangumiBase dataset is a comprehensive collection of anime and manga information.
This module extends the IncrementIDDataPool to specifically handle the BangumiBase dataset,
providing an easy-to-use interface for accessing and managing this data.
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

    :param revision: The specific revision of the BangumiBase dataset to use.
                     Defaults to 'main'.
    :type revision: str
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the BangumiBaseDataPool.

        :param revision: The specific revision of the BangumiBase dataset to use.
                         Defaults to 'main'.
        :type revision: str
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_BANGUMIBASE_REPO,
            data_revision=revision,
            idx_repo_id=_BANGUMIBASE_REPO,
            idx_revision=revision,
            hf_token=hf_token,
        )