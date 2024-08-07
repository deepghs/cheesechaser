import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import NozomiDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolNozomi:
    def test_nozomi_origin(self):
        with isolated_directory():
            pool = NozomiDataPool()
            pool.batch_download_to_directory(
                # 5 not exist
                resource_ids=[5, 1000, 20040000, 30000000, 30020000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('nozomi_5'))
