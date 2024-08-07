"""
This module provides a data pool implementation for Nozomi datasets.

It extends the functionality of the IncrementIDDataPool class to specifically handle
Nozomi datasets stored in a Hugging Face repository. The module defines a constant
for the repository name and a class that initializes the data pool with the
appropriate repository and revision information.
"""

from .base import IncrementIDDataPool

_NOZOMI_REPO = 'deepghs/nozomi_standalone_full'


class NozomiDataPool(IncrementIDDataPool):
    """
    A data pool class specifically designed for Nozomi datasets.

    This class inherits from IncrementIDDataPool and initializes it with
    the Nozomi-specific repository information. It provides a simple way to
    create a data pool for Nozomi datasets with optional revision specification.

    :param revision: The revision of the Nozomi dataset to use, defaults to 'main'
    :type revision: str

    Usage:
        >>> nozomi_pool = NozomiDataPool()  # Uses the 'main' revision
        >>> nozomi_pool_dev = NozomiDataPool(revision='dev')  # Uses the 'dev' revision
    """

    def __init__(self, revision: str = 'main'):
        """
        Initialize the NozomiDataPool with the specified revision.

        :param revision: The revision of the Nozomi dataset to use, defaults to 'main'
        :type revision: str
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_NOZOMI_REPO,
            data_revision=revision,
            idx_repo_id=_NOZOMI_REPO,
            idx_revision=revision,
        )
