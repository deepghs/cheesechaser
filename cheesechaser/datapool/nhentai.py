import json
import logging
import os.path
import shutil
from contextlib import contextmanager
from functools import lru_cache
from typing import ContextManager, Tuple, Any

import pandas as pd
from hbutils.system import TemporaryDirectory
from huggingface_hub.utils import LocalEntryNotFoundError

from .base import IncrementIDDataPool, DataPool, ResourceNotFoundError
from ..utils import get_hf_client

_DATA_REPO = 'deepghs/nhentai_full'


class NHentaiImagesDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_DATA_REPO,
            data_revision=revision,
            idx_repo_id=_DATA_REPO,
            idx_revision=revision,
            base_level=4,
        )


class NHentaiMangaDataPool(DataPool):
    def __init__(self, revision: str = 'main'):
        self.revision = revision
        self.images_pool = NHentaiImagesDataPool(revision=revision)

    @classmethod
    @lru_cache()
    def manga_id_map(cls, revision: str = 'main', local_files_prefer: bool = True):
        df = cls.manga_posts_table(revision, local_files_prefer)
        return {
            item['id']: json.loads(item['image_ids'])
            for item in df.to_dict('records')
        }

    @classmethod
    @lru_cache()
    def manga_posts_table(cls, revision: str = 'main', local_files_prefer: bool = True):
        client = get_hf_client()
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
    def mock_resource(self, resource_id, resource_info) -> ContextManager[Tuple[str, Any]]:
        maps = self.manga_id_map(self.revision, local_files_prefer=True)
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
