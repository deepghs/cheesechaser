import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import GelbooruWebpDataPool, GelbooruDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolGelbooru:
    def test_gelbooru_origin(self):
        with isolated_directory():
            pool = GelbooruDataPool()
            # 175 not exist
            pool.batch_download_to_directory(
                resource_ids=[120, 175, 3000000, 7000000, 7600000, 7800000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('gelbooru_5'))

    def test_gelbooru_webp(self):
        with isolated_directory():
            pool = GelbooruWebpDataPool()
            # 175 not exist
            pool.batch_download_to_directory(
                resource_ids=[120, 175, 3000000, 7000000, 7600000, 7800000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('gelbooru_webp_5'))
