import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import BangumiBaseDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolBangumiBase:
    def test_bangumibase_origin(self):
        with isolated_directory():
            pool = BangumiBaseDataPool()
            pool.batch_download_to_directory(
                resource_ids=range(2000000, 2000005),
                dst_dir='.',
            )

            dir_compare('.', get_testfile('bangumibase_5'))
