"""
This module provides a data pool implementation for Nozomi datasets.

It extends the functionality of the IncrementIDDataPool class to specifically handle
Nozomi datasets stored in a Hugging Face repository. The module defines a constant
for the repository name and a class that initializes the data pool with the
appropriate repository and revision information.

.. note::
    The dataset `deepghs/nozomi_standalone_full <https://huggingface.co/datasets/deepghs/nozomi_standalone_full>`_
    is gated, you have to get the access of it before using this module.
"""

from typing import Optional

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
    :param hf_token: Optional Hugging Face authentication token
    :type hf_token: Optional[str]

    Usage:
        >>> nozomi_pool = NozomiDataPool()  # Uses the 'main' revision
        >>> nozomi_pool_dev = NozomiDataPool(revision='dev')  # Uses the 'dev' revision
        >>> nozomi_pool_auth = NozomiDataPool(hf_token='your_token_here')  # Uses authentication
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the NozomiDataPool with the specified revision and optional authentication token.

        This method sets up the data pool using the Nozomi-specific repository
        and the provided revision. It also allows for optional authentication
        using a Hugging Face token.

        :param revision: The revision of the Nozomi dataset to use, defaults to 'main'
        :type revision: str
        :param hf_token: Optional Hugging Face authentication token for accessing private repositories
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_NOZOMI_REPO,
            data_revision=revision,
            idx_repo_id=_NOZOMI_REPO,
            idx_revision=revision,
            hf_token=hf_token,
        )
