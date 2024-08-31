"""
This module provides a data pool implementation for Yande image data.

It extends the IncrementIDDataPool class to specifically handle Yande image data,
utilizing a predefined Hugging Face repository for data storage and indexing.
"""

from typing import Optional

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
    :param hf_token: Optional Hugging Face authentication token.
    :type hf_token: Optional[str]

    :ivar data_repo_id: The Hugging Face repository ID for data storage.
    :ivar idx_repo_id: The Hugging Face repository ID for indexing.

    Usage:
        >>> yande_pool = YandeDataPool()
        >>> yande_pool_with_token = YandeDataPool(hf_token='your_token_here')

    .. note::
        This class assumes that both data and index information are stored
        in the same repository (_YANDE_REPO).
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the YandeDataPool.

        :param revision: The revision of the data to use, defaults to 'main'.
        :type revision: str
        :param hf_token: Optional Hugging Face authentication token for accessing private repositories.
        :type hf_token: Optional[str]

        This constructor initializes the YandeDataPool by calling the parent class constructor
        with specific parameters for the Yande data repository. It sets up both the data and
        index repositories to use the same Hugging Face repository and revision.
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_YANDE_REPO,
            data_revision=revision,
            idx_repo_id=_YANDE_REPO,
            idx_revision=revision,
            hf_token=hf_token,
        )
