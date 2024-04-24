import json
import os
import warnings
from dataclasses import dataclass
from typing import Union

from PIL import Image

from .base import IncrementIDDataPool, InvalidResourceDataError, ResourceNotFoundError


@dataclass
class ImageData:
    id: Union[int, str]
    image: Image.Image


class ImageOnlyDataPool(IncrementIDDataPool):
    def retrieve_resource_data(self, resource_id):
        with self._mock_resource(resource_id) as td:
            files = os.listdir(td)
            if len(files) == 0:
                raise ResourceNotFoundError(f'Image not found for resource {resource_id!r}.')
            elif len(files) != 1:
                raise InvalidResourceDataError(f'Image file not unique for resource {resource_id!r}.')

            src_file = os.path.join(td, files[0])
            image = Image.open(src_file)
            image.load()

            return ImageData(resource_id, image)


@dataclass
class ImageJsonAttachedData:
    id: Union[int, str]
    image: Image.Image
    data: dict


class ImageJsonAttachedDataPool(IncrementIDDataPool):
    def retrieve_resource_data(self, resource_id):
        with self._mock_resource(resource_id) as td:
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

                return ImageJsonAttachedData(resource_id, image, json_data)
