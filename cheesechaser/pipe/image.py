"""
This module provides image processing pipes for retrieving and handling image resources.

It includes two main classes:

1. ``SimpleImagePipe``: For retrieving single image files.
2. ``DataAttachedImagePipe``: For retrieving image files along with associated JSON data.

These pipes work with a resource pool to mock and access resources, and handle various
scenarios such as missing files, multiple files, and attached data.

The module also defines a DataAttachedImage dataclass to represent images with optional
associated data.

Usage:
    >>> from cheesechaser.pipe import SimpleImagePipe, DataAttachedImagePipe
    >>>
    >>> # Create and use a SimpleImagePipe
    >>> simple_pipe = SimpleImagePipe(pool)
    >>> image = simple_pipe.retrieve(resource_id, resource_metainfo)
    >>>
    >>> # Create and use a DataAttachedImagePipe
    >>> data_pipe = DataAttachedImagePipe(pool)
    >>> image_with_data = data_pipe.retrieve(resource_id, resource_metainfo)
"""

import json
import mimetypes
import os
import warnings
from dataclasses import dataclass
from typing import Optional

from PIL import Image

from .base import Pipe
from ..datapool import ResourceNotFoundError, InvalidResourceDataError


class SimpleImagePipe(Pipe):
    """
    A pipe for retrieving single image files from a resource pool.

    This pipe searches for image files in the given resource, opens the first
    found image file, and returns it as a PIL Image object.

    :raises ResourceNotFoundError: If no image file is found in the resource.
    :raises InvalidResourceDataError: If multiple image files are found in the resource.
    """

    def retrieve(self, resource_id, resource_metainfo, silent: bool = False):
        """
        Retrieve an image from the resource pool.

        :param resource_id: The identifier of the resource to retrieve.
        :param resource_metainfo: Metadata information about the resource.
        :param silent: If True, suppresses progress bar of each standalone files during the mocking process.
        :type silent: bool
        :return: A PIL Image object of the retrieved image.
        :rtype: PIL.Image.Image
        :raises ResourceNotFoundError: If no image file is found.
        :raises InvalidResourceDataError: If multiple image files are found.
        """
        with self.pool.mock_resource(resource_id, resource_metainfo, silent=silent) as (td, resource_metainfo):
            files = os.listdir(td)
            image_files = []
            for file in files:
                mimetype, _ = mimetypes.guess_type(file)
                if not mimetype or mimetype.startswith('image/'):
                    image_files.append(file)
            if len(image_files) == 0:
                raise ResourceNotFoundError(f'Image not found for resource {resource_id!r}.')
            elif len(image_files) != 1:
                raise InvalidResourceDataError(f'Image file not unique for resource {resource_id!r} '
                                               f'- {image_files!r}.')

            src_file = os.path.join(td, image_files[0])
            image = Image.open(src_file)
            image.load()
            return image


@dataclass
class DataAttachedImage:
    """
    A dataclass representing an image with optional attached data.

    :param image: The PIL Image object.
    :param data: Optional dictionary containing additional data associated with the image.
    """
    image: Image.Image
    data: Optional[dict]


class DataAttachedImagePipe(Pipe):
    """
    A pipe for retrieving image files along with associated JSON data from a resource pool.

    This pipe searches for an image file and an optional JSON file in the given resource.
    It returns a DataAttachedImage object containing the image and any associated data.

    :raises ResourceNotFoundError: If no image file is found in the resource.
    :raises InvalidResourceDataError: If multiple image files or JSON files are found in the resource.
    """

    def retrieve(self, resource_id, resource_metainfo, silent: bool = False):
        """
        Retrieve an image and its associated data from the resource pool.

        :param resource_id: The identifier of the resource to retrieve.
        :param resource_metainfo: Metadata information about the resource.
        :param silent: If True, suppresses progress bar of each standalone files during the mocking process.
        :type silent: bool
        :return: A DataAttachedImage object containing the image and any associated data.
        :rtype: DataAttachedImage
        :raises ResourceNotFoundError: If no image file is found.
        :raises InvalidResourceDataError: If multiple image files or JSON files are found.
        """
        with self.pool.mock_resource(resource_id, resource_metainfo, silent=silent) as (td, resource_metainfo):
            files = os.listdir(td)
            if len(files) == 0:
                raise ResourceNotFoundError(f'Image not found for resource {resource_id!r}.')
            else:
                json_files = [file for file in files if file.lower().endswith('.json')]
                non_json_files = [file for file in files if not file.lower().endswith('.json')]

                if not non_json_files:
                    raise ResourceNotFoundError(
                        f'Image file not found, but json data found for resource {resource_id!r}.')
                elif len(non_json_files) > 1:
                    raise InvalidResourceDataError(f'Image files not unique for resource {resource_id!r}.')
                else:
                    image_file = os.path.join(td, non_json_files[0] if non_json_files else None)

                if not json_files:
                    warnings.warn(f'Json data file not found for resource {resource_id!r}.')
                    json_file = None
                elif len(json_files) > 1:
                    raise InvalidResourceDataError(f'Json data files not unique for resource {resource_id!r}.')
                else:
                    json_file = os.path.join(td, json_files[0] if json_files else None)

                image = Image.open(image_file)
                image.load()
                if json_file:
                    with open(json_file, 'r') as f:
                        json_data = json.load(f)
                else:
                    json_data = None

                return DataAttachedImage(image, json_data)
