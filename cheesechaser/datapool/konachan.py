"""
This module provides a data pool implementation for Konachan images.

It defines the KonachanDataPool class, which is a specialized version of the
IncrementIDDataPool for handling Konachan image data. The module uses a
predefined repository for storing and accessing Konachan image data.
"""

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

    Usage:
        >>> konachan_pool = KonachanDataPool()
    """

    def __init__(self, revision: str = 'main'):
        """
        Initialize the KonachanDataPool.

        :param revision: The revision of the data to use, defaults to 'main'.
        :type revision: str
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_KONACHAN_REPO,
            data_revision=revision,
            idx_repo_id=_KONACHAN_REPO,
            idx_revision=revision,
        )
