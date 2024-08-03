import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import DanbooruNewestWebpDataPool, DanbooruNewestDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolDanbooru:
    def test_danbooru_origin(self):
        with isolated_directory():
            pool = DanbooruNewestDataPool()
            # 120 not exist
            pool.batch_download_to_directory(
                resource_ids=[120, 175, 5000000, 7000000, 7600000, 7800000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('danbooru_5'))

    def test_download_webp(self):
        with isolated_directory():
            pool = DanbooruNewestWebpDataPool()
            # 120 not exist
            pool.batch_download_to_directory(
                resource_ids=[120, 175, 5000000, 7000000, 7600000, 7800000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('danbooru_webp_5'))
