import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import AnimePicturesDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolAnimePictures:
    def test_anime_pictures_origin(self):
        with isolated_directory():
            pool = AnimePicturesDataPool()
            pool.batch_download_to_directory(
                resource_ids=range(200000, 200005),
                dst_dir='.',
            )

            dir_compare('.', get_testfile('anime_pictures_2'))
