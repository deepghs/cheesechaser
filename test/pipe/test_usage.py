import pytest
from PIL import Image

from cheesechaser.datapool import DanbooruNewestWebpDataPool
from cheesechaser.pipe import SimpleImagePipe
from ..testings import get_testfile


@pytest.mark.unittest
class TestPipeUsage:
    def test_simple_actual_usage_on_danbooru(self, image_diff):
        pool = DanbooruNewestWebpDataPool()
        pipe = SimpleImagePipe(pool)

        ids = [120, 175, 5000000, 7000000, 7600000, 7800000]
        image_count = 0
        with pipe.batch_retrieve(ids) as session:
            for item in session:
                image = Image.open(get_testfile('danbooru_webp_5', f'{item.id}.webp'))
                assert image_diff(image, item.data, throw_exception=False) < 1e-2
                image_count += 1
            assert image_count == 5

    def test_actual_usage_with_100_images(self):
        pool = DanbooruNewestWebpDataPool()
        pipe = SimpleImagePipe(pool)

        ids = range(7000000, 7700000, 7000)
        with pipe.batch_retrieve(ids) as session:
            image_count = 0
            for item in session:
                assert isinstance(item.data, Image.Image)
                image_count += 1

            assert image_count >= 90
