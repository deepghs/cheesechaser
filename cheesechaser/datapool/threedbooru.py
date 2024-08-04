"""
This module provides a data pool implementation for the 3D Booru dataset.

It includes a class `ThreedbooruDataPool` which extends the `IncrementIDDataPool`
to specifically handle the 3D Booru dataset. This module allows users to easily
access and manage data from the 3D Booru repository hosted on Hugging Face.
"""

from .base import IncrementIDDataPool

_3DBOORU_REPO = 'deepghs/3dbooru_full'


class ThreedbooruDataPool(IncrementIDDataPool):
    """
    A data pool class for managing and accessing the 3D Booru dataset.

    This class extends the `IncrementIDDataPool` to provide specific functionality
    for the 3D Booru dataset. It uses a predefined repository ID for both data
    and index information.

    :param revision: The specific revision of the 3D Booru dataset to use, defaults to 'main'.
    :type revision: str

    Usage:
        >>> pool = ThreedbooruDataPool()

    .. note::
        The 3D Booru dataset is hosted in the 'deepghs/3dbooru_full' repository on Hugging Face.
    """

    def __init__(self, revision: str = 'main'):
        """
        Initialize the ThreedbooruDataPool.

        :param revision: The specific revision of the 3D Booru dataset to use, defaults to 'main'.
        :type revision: str
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_3DBOORU_REPO,
            data_revision=revision,
            idx_repo_id=_3DBOORU_REPO,
            idx_revision=revision,
        )
