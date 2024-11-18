import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import Rule34WebpDataPool, Rule34DataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolRule34:
    def test_rule34_origin(self):
        with isolated_directory():
            pool = Rule34DataPool()
            pool.batch_download_to_directory(
                resource_ids=[120, 175, 3000000, 7000000, 7600000, 7800000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('rule34_6'))

    def test_rule34_webp(self):
        with isolated_directory():
            pool = Rule34WebpDataPool()
            pool.batch_download_to_directory(
                resource_ids=[120, 175, 3000000, 7000000, 7600000, 7800000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('rule34_webp_6'))
