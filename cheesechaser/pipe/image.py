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
    def retrieve(self, resource_id, resource_metainfo):
        with self.pool.mock_resource(resource_id, resource_metainfo) as (td, resource_metainfo):
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
    image: Image.Image
    data: Optional[dict]


class DataAttachedImagePipe(Pipe):
    def retrieve(self, resource_id, resource_metainfo):
        with self.pool.mock_resource(resource_id, resource_metainfo) as (td, resource_metainfo):
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
