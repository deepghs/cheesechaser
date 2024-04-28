from .base import IncrementIDDataPool

_3DBOORU_REPO = 'deepghs/3dbooru_full'


class ThreedbooruDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_3DBOORU_REPO,
            data_revision=revision,
            idx_repo_id=_3DBOORU_REPO,
            idx_revision=revision,
        )
