import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import E621NewestDataPool, E621NewestWebpDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolE621:
    def test_e621_origin(self):
        with isolated_directory():
            pool = E621NewestDataPool()
            # 69 not exist
            pool.batch_download_to_directory(
                resource_ids=[68, 69, 93, 97, 5080080, 5080082, 5080086],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('e621_5'))

    def test_download_webp(self):
        with isolated_directory():
            pool = E621NewestWebpDataPool()
            # 69 not exist
            # 97 if a gif image
            pool.batch_download_to_directory(
                resource_ids=[68, 69, 93, 97, 5080080, 5080082, 5080086],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('e621_webp_5'))
