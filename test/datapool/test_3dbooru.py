import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import ThreedbooruDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolThreedbooru:
    def test_3dbooru_origin(self):
        with isolated_directory():
            pool = ThreedbooruDataPool()
            pool.batch_download_to_directory(
                resource_ids=range(500000, 500003),
                dst_dir='.',
            )

            dir_compare('.', get_testfile('3dbooru_3'))
