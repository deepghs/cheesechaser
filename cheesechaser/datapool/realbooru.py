"""
This module provides a data pool implementation for Realbooru dataset.

The RealbooruDataPool class extends the IncrementIDDataPool to specifically handle
the Realbooru dataset, which is stored in a Hugging Face repository.
"""

from .base import IncrementIDDataPool

_REALBOORU_REPO = 'deepghs/realbooru_full'


class RealbooruDataPool(IncrementIDDataPool):
    """
    A data pool class for accessing and managing Realbooru dataset.

    This class inherits from IncrementIDDataPool and is specifically designed to work
    with the Realbooru dataset stored in a Hugging Face repository. It provides an
    interface to access and manage the data using incremental IDs.

    :param revision: The specific revision of the Realbooru dataset to use, defaults to 'main'.
    :type revision: str
    """

    def __init__(self, revision: str = 'main'):
        """
        Initialize the RealbooruDataPool.

        :param revision: The specific revision of the Realbooru dataset to use, defaults to 'main'.
        :type revision: str
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_REALBOORU_REPO,
            data_revision=revision,
            idx_repo_id=_REALBOORU_REPO,
            idx_revision=revision,
        )
