import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import RealbooruDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolRealbooru:
    def test_realbooru_origin(self):
        with isolated_directory():
            pool = RealbooruDataPool()
            pool.batch_download_to_directory(
                resource_ids=range(500000, 500003),
                dst_dir='.',
            )

            dir_compare('.', get_testfile('realbooru_3'))
