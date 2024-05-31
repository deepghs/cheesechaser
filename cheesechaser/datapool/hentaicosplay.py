import os
from typing import Iterable

from natsort import natsorted

from cheesechaser.datapool import IncrementIDDataPool
from cheesechaser.datapool.base import id_modulo_cut
from cheesechaser.utils import get_hf_fs

_HC_REPO = 'deepghs/hentai_cosplay_trans'


class HentaiCosplayDataPool(IncrementIDDataPool):
    def __init__(self, repo_id: str = _HC_REPO, revision: str = 'main', base_level: int = 3):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=repo_id, data_revision=revision,
            idx_repo_id=repo_id, idx_revision=revision,
            base_level=base_level,
        )
        self._archive_dirs = None

    def _get_archive_dirs(self):
        if self._archive_dirs is None:
            hf_fs = get_hf_fs()
            self._archive_dirs = natsorted({
                os.path.dirname(os.path.relpath(file, f'datasets/{self.data_repo_id}'))
                for file in hf_fs.glob(f'datasets/{self.data_repo_id}/**/*.tar')
            })

        return self._archive_dirs

    def _request_possible_archives(self, resource_id) -> Iterable[str]:
        modulo = resource_id % (10 ** self.base_level)
        modulo_str = str(modulo)
        if len(modulo_str) < self.base_level:
            modulo_str = '0' * (self.base_level - len(modulo_str)) + modulo_str

        modulo_segments = id_modulo_cut(modulo_str)
        modulo_segments[-1] = f'{modulo_segments[-1]}'
        return [
            f'{base_dir}/{"/".join(modulo_segments)}.tar'
            for base_dir in self._get_archive_dirs()
        ]
