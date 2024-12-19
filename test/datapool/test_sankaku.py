import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import SankakuWebpDataPool, SankakuDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolSankaku:
    def test_sankaku_origin(self):
        with isolated_directory():
            pool = SankakuDataPool()
            pool.batch_download_to_directory(
                resource_ids=[4000000, 4000001, 4000002, 36863304, 36863146],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('sankaku_5'))

    def test_sankaku_webp(self):
        with isolated_directory():
            pool = SankakuWebpDataPool()
            pool.batch_download_to_directory(
                resource_ids=[4000000, 4000001, 4000002, 36863304, 36863146],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('sankaku_webp_5'))
