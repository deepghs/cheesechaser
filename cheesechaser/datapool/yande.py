"""
This module provides a data pool implementation for Yande image data.

It extends the IncrementIDDataPool class to specifically handle Yande image data,
utilizing a predefined Hugging Face repository for data storage and indexing.
"""

from .base import IncrementIDDataPool

_YANDE_REPO = 'deepghs/yande_full'


class YandeDataPool(IncrementIDDataPool):
    """
    A data pool class for managing Yande image data.

    This class extends IncrementIDDataPool to provide a specialized implementation
    for handling Yande image data. It uses a predefined Hugging Face repository
    for both data storage and indexing.

    :param revision: The revision of the data to use, defaults to 'main'.
    :type revision: str

    Usage:
        >>> yande_pool = YandeDataPool()

    .. note::
        This class assumes that both data and index information are stored
        in the same repository (_YANDE_REPO).
    """

    def __init__(self, revision: str = 'main'):
        """
        Initialize the YandeDataPool.

        :param revision: The revision of the data to use, defaults to 'main'.
        :type revision: str
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_YANDE_REPO,
            data_revision=revision,
            idx_repo_id=_YANDE_REPO,
            idx_revision=revision,
        )
