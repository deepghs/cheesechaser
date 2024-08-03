import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import ZerochanWebpDataPool, ZerochanDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolZerochan:
    def test_zerochan_origin(self):
        with isolated_directory():
            pool = ZerochanDataPool()
            # 175 not exist
            pool.batch_download_to_directory(
                resource_ids=[175, 190, 2000000, 3000000, 3600000, 4200000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('zerochan_5'))

    def test_zerochan_webp(self):
        with isolated_directory():
            pool = ZerochanWebpDataPool()
            # 175 not exist
            pool.batch_download_to_directory(
                resource_ids=[175, 190, 2000000, 3000000, 3600000, 4200000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('zerochan_webp_5'))
