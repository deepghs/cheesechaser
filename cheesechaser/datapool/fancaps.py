"""
This module provides a data pool implementation for the Fancaps dataset.

It includes a class `FancapsDataPool` which extends the `IncrementIDDataPool` base class.
The module is designed to facilitate access to the Fancaps dataset, which is hosted on
the Hugging Face Hub.

The constant `_FANCAPS_REPO` defines the repository ID for the Fancaps dataset.
"""

from .base import IncrementIDDataPool

_FANCAPS_REPO = 'deepghs/fancaps_full'


class FancapsDataPool(IncrementIDDataPool):
    """
    A data pool class for accessing and managing the Fancaps dataset.

    This class extends the `IncrementIDDataPool` base class and is specifically
    tailored for the Fancaps dataset. It provides an interface to access the
    dataset stored in the Hugging Face repository.

    :param revision: The specific revision of the dataset to use, defaults to 'main'.
    :type revision: str

    Usage:
        >>> fancaps_pool = FancapsDataPool()

    .. note::
        The Fancaps dataset is stored in the repository defined by `_FANCAPS_REPO`.
        Both the data and index are stored in the same repository.
    """

    def __init__(self, revision: str = 'main'):
        """
        Initialize the FancapsDataPool.

        :param revision: The specific revision of the dataset to use, defaults to 'main'.
        :type revision: str
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_FANCAPS_REPO,
            data_revision=revision,
            idx_repo_id=_FANCAPS_REPO,
            idx_revision=revision,
        )
