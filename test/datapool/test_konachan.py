import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import KonachanDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolKonachan:
    def test_konachan_origin(self):
        with isolated_directory():
            pool = KonachanDataPool()
            pool.batch_download_to_directory(
                resource_ids=range(200000, 200003),
                dst_dir='.',
            )

            dir_compare('.', get_testfile('konachan_3'))
