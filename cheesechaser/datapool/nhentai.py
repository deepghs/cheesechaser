"""
This module provides data pool classes for managing and accessing NHentai manga and image data.

The module includes two main classes:

1. NHentaiImagesDataPool: A data pool for managing NHentai images.
2. NHentaiMangaDataPool: A data pool for managing NHentai manga data, including image associations.

These classes provide functionality for retrieving manga information, downloading images,
and managing resources from a Hugging Face dataset repository.

.. note::
    The dataset `deepghs/nhentai_full <https://huggingface.co/datasets/deepghs/nhentai_full>`_
    is gated, you have to get the access of it before using this module.
"""

import json
import logging
import os.path
import shutil
from contextlib import contextmanager
from functools import lru_cache
from threading import Lock
from typing import ContextManager, Tuple, Any, Optional

import pandas as pd
from hbutils.system import TemporaryDirectory
from hfutils.operate import get_hf_client
from huggingface_hub.utils import LocalEntryNotFoundError

from .base import IncrementIDDataPool, DataPool, ResourceNotFoundError

_DATA_REPO = 'deepghs/nhentai_full'


class NHentaiImagesDataPool(IncrementIDDataPool):
    """
    A data pool class for managing NHentai images.

    This class extends the IncrementIDDataPool to provide specific functionality
    for handling NHentai image data. It allows for efficient retrieval and management
    of image resources from a Hugging Face dataset repository.

    :param revision: The revision of the data to use, defaults to 'main'.
    :type revision: str
    :param hf_token: Hugging Face API token for authentication, defaults to None.
    :type hf_token: Optional[str]

    Usage:
        images_pool = NHentaiImagesDataPool(revision='latest')
        # Use images_pool to access and manage NHentai images
    """

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the NHentaiImagesDataPool.

        :param revision: The revision of the data to use, defaults to 'main'.
        :type revision: str
        :param hf_token: Hugging Face API token for authentication, defaults to None.
        :type hf_token: Optional[str]
        """
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_DATA_REPO,
            data_revision=revision,
            idx_repo_id=_DATA_REPO,
            idx_revision=revision,
            base_level=4,
            hf_token=hf_token,
        )


class NHentaiMangaDataPool(DataPool):
    """
    A data pool class for managing NHentai manga data.

    This class provides methods for retrieving manga information, downloading associated images,
    and managing manga resources. It utilizes the NHentaiImagesDataPool for handling image data.

    :param revision: The revision of the data to use, defaults to 'main'.
    :type revision: str
    :param hf_token: Hugging Face API token for authentication, defaults to None.
    :type hf_token: Optional[str]

    Usage:
        manga_pool = NHentaiMangaDataPool(revision='latest')
        # Use manga_pool to access manga information and associated images
    """

    __data_lock__ = Lock()

    def __init__(self, revision: str = 'main', hf_token: Optional[str] = None):
        """
        Initialize the NHentaiMangaDataPool.

        :param revision: The revision of the data to use, defaults to 'main'.
        :type revision: str
        :param hf_token: Hugging Face API token for authentication, defaults to None.
        :type hf_token: Optional[str]
        """
        self.revision = revision
        self.images_pool = NHentaiImagesDataPool(revision=revision, hf_token=hf_token)
        self._hf_token = hf_token

    @classmethod
    @lru_cache()
    def manga_id_map(cls, revision: str = 'main', local_files_prefer: bool = True,
                     hf_token: Optional[str] = None):
        """
        Get a mapping of manga IDs to their associated image IDs.

        This method is cached for efficiency and provides a quick lookup for manga-to-image associations.

        :param revision: The revision of the data to use, defaults to 'main'.
        :type revision: str
        :param local_files_prefer: Whether to prefer local files, defaults to True.
        :type local_files_prefer: bool
        :param hf_token: Hugging Face API token for authentication, defaults to None.
        :type hf_token: Optional[str]
        :return: A dictionary mapping manga IDs to lists of image IDs.
        :rtype: dict
        """
        df = cls.manga_posts_table(revision, local_files_prefer, hf_token=hf_token)
        return {
            item['id']: json.loads(item['image_ids'])
            for item in df.to_dict('records')
        }

    @classmethod
    @lru_cache()
    def manga_posts_table(cls, revision: str = 'main', local_files_prefer: bool = True,
                          hf_token: Optional[str] = None):
        """
        Retrieve the manga posts table as a pandas DataFrame.

        This method is cached for efficiency and provides access to the complete manga post information.

        :param revision: The revision of the data to use, defaults to 'main'.
        :type revision: str
        :param local_files_prefer: Whether to prefer local files, defaults to True.
        :type local_files_prefer: bool
        :param hf_token: Hugging Face API token for authentication, defaults to None.
        :type hf_token: Optional[str]
        :return: A pandas DataFrame containing manga post information.
        :rtype: pandas.DataFrame
        """
        client = get_hf_client(hf_token=hf_token)
        try:
            csv_file = client.hf_hub_download(
                repo_id=_DATA_REPO,
                repo_type='dataset',
                revision=revision,
                filename='posts.csv',
                local_files_only=True if local_files_prefer else False,
            )
        except LocalEntryNotFoundError:
            csv_file = client.hf_hub_download(
                repo_id=_DATA_REPO,
                repo_type='dataset',
                revision=revision,
                filename='posts.csv',
                local_files_only=False,
            )

        return pd.read_csv(csv_file)

    @contextmanager
    def mock_resource(self, resource_id, resource_info, silent: bool = False) -> ContextManager[Tuple[str, Any]]:
        """
        Create a mock resource for a given manga.

        This method downloads the associated images for a manga and organizes them
        in a temporary directory. It's useful for processing or analyzing manga content.

        :param resource_id: The ID of the manga resource.
        :type resource_id: int
        :param resource_info: Additional information about the resource.
        :type resource_info: Any
        :param silent: If True, suppresses progress bar of each standalone files during the mocking process.
        :type silent: bool
        :yield: A tuple containing the path to the temporary directory with the images and the resource info.
        :rtype: Tuple[str, Any]
        :raises ResourceNotFoundError: If the specified manga resource is not found.
        """
        with self.__data_lock__:
            maps = self.manga_id_map(self.revision, local_files_prefer=True, hf_token=self._hf_token)
        if resource_id not in maps:
            raise ResourceNotFoundError(f'Manga {resource_id!r} not found.')

        with TemporaryDirectory() as td:
            origin_dir = os.path.join(td, 'origin')
            os.makedirs(origin_dir, exist_ok=True)
            image_ids = maps[resource_id]
            logging.info(f'Images {image_ids!r} found for manga resource {resource_id}.')
            self.images_pool.batch_download_to_directory(
                image_ids, origin_dir,
                save_metainfo=False,
                silent=silent,
            )
            files = {}
            for src_image_file in os.listdir(origin_dir):
                body, _ = os.path.splitext(os.path.basename(src_image_file))
                files[int(body)] = src_image_file

            dst_dir = os.path.join(td, 'dst')
            os.makedirs(dst_dir, exist_ok=True)
            missing_ids = []
            for i, image_id in enumerate(image_ids, start=1):
                if image_id in files:
                    src_file = os.path.join(origin_dir, files[image_id])
                    _, ext = os.path.splitext(src_file)
                    dst_file = os.path.join(dst_dir, f'{resource_id}_p{i}{ext}')
                    shutil.move(src_file, dst_file)
                else:
                    missing_ids.append(i)

            if missing_ids:
                logging.info(f'Image {missing_ids!r} not found for resource {resource_id!r}.')
            yield dst_dir, resource_info
