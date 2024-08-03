import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import YandeDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolYande:
    def test_yande_origin(self):
        with isolated_directory():
            pool = YandeDataPool()
            pool.batch_download_to_directory(
                resource_ids=range(500000, 500003),
                dst_dir='.',
            )

            dir_compare('.', get_testfile('yande_3'))
