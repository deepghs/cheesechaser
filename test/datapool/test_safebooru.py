import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import SafebooruWebpDataPool, SafebooruDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolSafebooru:
    def test_safebooru_origin(self):
        with isolated_directory():
            pool = SafebooruDataPool()
            # 4000084 not exist
            pool.batch_download_to_directory(
                resource_ids=[4000000, 4000001, 4000002, 4000003, 4000084],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('safebooru_5'))

    def test_safebooru_webp(self):
        with isolated_directory():
            pool = SafebooruWebpDataPool()
            # 4000084 not exist
            pool.batch_download_to_directory(
                resource_ids=[4000000, 4000001, 4000002, 4000003, 4000084],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('safebooru_webp_5'))
