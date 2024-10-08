import logging

import pytest

from cheesechaser.datapool import NozomiDataPool
from cheesechaser.pipe import SimpleImagePipe
from cheesechaser.query import NozomiIdQuery
from ..testings import is_character_ratio


@pytest.mark.unittest
class TestQueryNozomi:
    def test_query_nozomi(self):
        pool = NozomiDataPool()
        pipe = SimpleImagePipe(pool)

        with pipe.batch_retrieve(NozomiIdQuery(['surtr_(arknights)', 'solo'])) as session:
            image_count = 0
            is_character_count = 0
            for item in session:
                ratio = is_character_ratio(item.data, 'surtr_(arknights)')
                if ratio >= 0.75:
                    is_character_count += 1
                else:
                    logging.warning(f'Resource #{item.id} is not the expected character - {ratio}.')

                image_count += 1
                if image_count >= 10:
                    break

            assert image_count >= 10, f'Image count not enough - {image_count!r}.'
            assert is_character_count >= 7, f'Only {is_character_count} of {image_count} image(s) ' \
                                            f'are the expected character.'
