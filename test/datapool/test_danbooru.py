import pytest
from hbutils.testing import isolated_directory

from cheesechaser.datapool import DanbooruNewestWebpDataPool, DanbooruNewestDataPool, Danbooru2024SfwDataPool, \
    Danbooru2024DataPool, Danbooru2024WebpDataPool
from ..testings import get_testfile, dir_compare


@pytest.mark.unittest
class TestDatapoolDanbooru:
    def test_danbooru_origin(self):
        with isolated_directory():
            pool = DanbooruNewestDataPool()
            # 120 not exist
            pool.batch_download_to_directory(
                resource_ids=[120, 175, 5000000, 7000000, 7600000, 7800000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('danbooru_5'))

    def test_download_webp(self):
        with isolated_directory():
            pool = DanbooruNewestWebpDataPool()
            # 120 not exist
            pool.batch_download_to_directory(
                resource_ids=[120, 175, 5000000, 7000000, 7600000, 7800000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('danbooru_webp_5'))

    def test_danbooru2024_sfw(self):
        with isolated_directory():
            pool = Danbooru2024SfwDataPool()
            # 120 not exist
            # 7800000 is nsfw
            pool.batch_download_to_directory(
                resource_ids=[120, 175, 5000000, 7000000, 7600000, 7800000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('danbooru2024_sfw_4'))

    def test_danbooru2024(self):
        with isolated_directory():
            pool = Danbooru2024DataPool()
            # 120 not exist
            pool.batch_download_to_directory(
                resource_ids=[120, 175, 5000000, 7000000, 7600000, 7800000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('danbooru2024_5'))

    def test_danbooru2024_webp(self):
        with isolated_directory():
            pool = Danbooru2024WebpDataPool()
            # 120 not exist
            pool.batch_download_to_directory(
                resource_ids=[120, 175, 5000000, 7000000, 7600000, 7800000],
                dst_dir='.',
            )

            dir_compare('.', get_testfile('danbooru2024_webp_5'))
