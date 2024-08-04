"""
This module provides a data pool implementation for Civitai, a platform for AI-generated art and models.

The CivitaiDataPool class extends the IncrementIDDataPool to specifically handle data from Civitai.
It uses a predefined repository to store and retrieve data.
"""

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

    Usage:
        >>> civitai_pool = CivitaiDataPool()
    """

    def __init__(self, revision: str = 'main'):
        """
        Initialize the CivitaiDataPool with the specified revision.

        :param revision: The specific revision of the data to use, defaults to 'main'.
        :type revision: str
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_CIVITAI_REPO,
            data_revision=revision,
            idx_repo_id=_CIVITAI_REPO,
            idx_revision=revision,
            base_level=4,
        )
