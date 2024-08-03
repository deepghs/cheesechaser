import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import FancapsDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolFancaps:
    def test_fancaps_origin(self):
        with isolated_directory():
            pool = FancapsDataPool()
            pool.batch_download_to_directory(
                resource_ids=range(7000000, 7000005),
                dst_dir='.',
            )

            dir_compare('.', get_testfile('fancaps_5'))
